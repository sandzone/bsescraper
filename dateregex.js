import {promises as fs} from 'fs'
import {parse} from 'csv-parse/sync';

const conference_call = "(earnings|results)\\s*conference\\s*call\\s*(transcript)?"
const year = "(?<year>(\\d{4}|\\d{2})|((?<fcy>(f|c)y)(’|-|')?\\s*(\\d{4}|\\d{2})))" //numbers or fy or cy followed by ' - \\s
const quarter = "(?<quarter>([1-4]\\s*q)|(q\\s*[1-4]))?"
const half = "(?<half>([1-4]\\s*h)|(h\\s*[1-4]\\s))?"
const nonHalf = "(&\\s*[a-z|\\s]{1,20})?" // to capture things like q3 and nine months etc.

const months = '('+['january','february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december'].join('|')+')'
const mmmm_dd_yyyy_string = '(?<date>'+months+'\\s*([1-9]|[12][0-9]|3[01])\\s*,?\\s*(\\d{4}|\\d{2}))'
//https://www.regextutorial.org/regex-for-numbers-and-ranges.php
const patternTwo = '('+conference_call+'\\s*'+mmmm_dd_yyyy_string+')'
const patternOne = '('+quarter+'\\s*&?\\s*'+half+'\\s*'+nonHalf+'\\s*'+year+'\\s*'+conference_call+')'

const date_regex = new RegExp(patternOne+'|'+patternTwo,'ig')

function getYear(year)  {
  if (year>100 && year<2000) year = year%1900
  if (year>100 && year>=2000) year = year%2000
  if (year<10) year = '0'+year.toString()
  else year = year.toString()
  return year
}

function getLastQuarter(dt) {
  let month = dt.getMonth()+1,
      day = dt.getDate(),
      year = ((month-3)<=0?dt.getFullYear()-1:dt.getFullYear()),
      prvQuarter = Math.ceil(((month-3)<=0?12:(month-3))/3);

  return prvQuarter.toString()+'QCY'+getYear(year)
}

function getTranscriptDate(date_string, news_dt) {
  const match = date_regex.exec(date_string)
  let quarter = '', year = '';
  date_regex.lastIndex = 0;
  if (match) {
    if (match.groups.quarter && match.groups.year) {
      quarter = new RegExp('\\d+','ig').exec(match.groups.quarter)[0]
      year = getYear(parseInt(new RegExp('\\d+','ig').exec(match.groups.year)[0]))
      return quarter+"Q"+match.groups.fcy+year
    }

    if (match.groups.half && match.groups.year) {
      half = new RegExp('\\d+','ig').exec(match.groups.half)[0]
      year = getYear(parseInt(new RegExp('\\d+','ig').exec(match.groups.year)[0]))
      return half+"H"+match.groups.fcy+year
    }

    if (match.groups.date)  {
      return getLastQuarter(new Date(Date.parse(match.groups.date)))
    }

    return getLastQuarter(new Date(Date.parse(news_dt)))
  }
}

const dateregex = async()=>{
  const regexFile = await fs.readFile('./datepatterns.csv')
  const parsedCsv = parse(regexFile, {
                                      columns: false,
                                      skip_empty_lines: true
                                    });

  parsedCsv.forEach(d=>{
    console.log(d[0])
    console.log(getTranscriptDate(d[0], new Date().toISOString()))
    console.log("***********************")
  })

}

dateregex()
