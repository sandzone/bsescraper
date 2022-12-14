import {promises as fs} from 'fs'
import pdfjsLib from 'pdfjs-dist'
import clustering from 'density-clustering'
import levenshtein from 'fast-levenshtein'
import axios from 'axios';
import {uniqueId} from 'remirror';
import {MongoClient} from 'mongodb';
const mongo = new MongoClient("mongodb+srv://sandzone:%40Sdkram2k@cluster0.oik2vtl.mongodb.net/nwb?retryWrites=true&w=majority");

//https://naturalnode.github.io/natural/brill_pos_tagger.html
import natural from 'natural'
const language = "EN"
const defaultCategory = 'N';
const defaultCategoryCapitalized = 'NNP';
const lexicon = new natural.Lexicon(language, defaultCategory, defaultCategoryCapitalized);
const ruleSet = new natural.RuleSet('EN');
const tagger = new natural.BrillPOSTagger(lexicon, ruleSet);

const getLinkFromPdf = (op) =>  {
  const re=new RegExp('https://.*\.pdf', 'ig');
  let link = ''
  for (let i=0; i<op.length; i++) {
    const block = op[i]
    const match = re.exec(block.text)
    if (match)  {
      link = match[0].replace(' ','')
      break
    }
  }
  return link
}

function getDx(i,j)  {
  return Math.abs(i.cx-j.cx)
}

function getDy(i,j)  {
  return Math.abs(i.cy-j.cy)
}

function hasXYOverlap(i,j) {
  const [dx, dy] = [getDx(i,j), getDy(i,j)]
  return (dx<(i.width+j.width)/2 && dy<(i.height+j.height)/2)
}

function hasXOverlap(i,j)  {
  const [dx, dy] = [getDx(i,j), getDy(i,j)]
  return (dy<(i.height+j.height)/2)
}

function hasYOverlap(i,j)  {
  const [dx, dy] = [getDx(i,j), getDy(i,j)]
  return dx<(i.width+j.width)/2
}

const getDist = (i,j) =>  {
  if (hasXOverlap(i,j)) return 0
  if (hasYOverlap(i,j)) return getDy(i,j)-(i.height+j.height)/2
  const a = Math.abs(getDx(i,j)-(i.width+j.width)/2)
  const b = Math.abs(getDy(i,j)-(i.height+j.height)/2)
  return Math.sqrt(a*a+b*b)
}

const mergeBBox = (i,j)  =>  {
  const x1 = Math.min(i.x1, j.x1)
  const y1 = Math.min(i.y1, j.y1)
  const x2 = Math.max(i.x2, j.x2)
  const y2 = Math.max(i.y2, j.y2)
  const w = x2-x1
  const h = y2-y1
  const cx = x1+w/2
  const cy = y1+h/2
  const _x2 = j.x2  //assume that its always proceeding in a sequential manner
  return {
    cx:cx, cy:cy, width:w, height:h, x1:x1, y1:y1, x2:x2, _x2:_x2, y2:y2
  }
}

const clusterText = (textContent) =>  {
  let optics = new clustering.OPTICS();

  const lineClusters = optics.run(textContent, 15, 2, getDist)
  let results = [], boundingBox = {}

  lineClusters.forEach(cluster=>{
    let cum='', bbox={}
    cluster.sort((a,b)=>{
      if (a[1]==b[1]) return a[0]-b[0]
      return a[1]-b[1]
    })

    cluster.forEach((i,idx)=>{
      let spacer = ' '
      //console.log(cum, textContent[i].text)
      //console.log(bbox.cy, textContent, getDy(bbox, textContent[i]), bbox.height/2+textContent.height/2, hasXOverlap(bbox, textContent))

      if (hasXOverlap(Object.assign(Object.assign({},bbox),{x2:bbox._x2}), textContent[i])) {
        const distance = (textContent[i].x1-bbox._x2)<0.35*((textContent[i].x2-textContent[i].x1)/textContent[i].text.length)
        if (distance==true) spacer = ''
      }
      cum=idx==0?textContent[i].text:cum+spacer+textContent[i].text
      bbox = idx==0?mergeBBox(textContent[i],textContent[i]):mergeBBox(bbox,textContent[i])
    })
    results.push({text:cum.replace(/\s+/g,' ').trim(), bbox:bbox})
  })
  return results
}

const attachRepetition = (textItems) =>  {
  return textItems.map(item=>{
    const curPage = item.pageNo
    const otherPages = textItems.filter(d=>d.pageNo!=curPage)
    let repetition = 0

    if (item.text.replace(/\s+/g,'').length>0)  {
      otherPages.forEach(_item=>{
        if (hasXYOverlap(item, _item) && levenshtein.get(item.text, _item.text)<3)  {
          repetition++
        }
      })
    }
    return {...item, repetition:repetition}
  })
}

const canDelete = (item, totalPages) => {
  return item.repetition>Math.floor(0.75*totalPages)
}

const hasSpeaker = (text)  =>  {
  const splits = text.split(':')
  if (splits.length==1) return false
  const words = splits[0].split(' ')
  return tagger.tag(words).taggedWords.every(d=>d.tag==='NNP'||d.token.toLowerCase()==='moderator')
}

const mergePageTransitionBlocks = (blocks) => {
  let newBlocks = []
  for (let i=0; i<blocks.length; i++) {
    if (i+1==blocks.length) {
      newBlocks.push(blocks[i])
      break
    }
    if (blocks[i+1].pageNo==blocks[i].pageNo+1) {
      if (!hasSpeaker(blocks[i+1].text))  {
        newBlocks.push({text:(blocks[i].text+' '+blocks[i+1].text).replace(/\s+/g,' ')})
        i++
      }
    }
    else
      newBlocks.push(blocks[i])
  }
  return newBlocks
}

const markForExtraction = (blocks)  =>  {
  let newBlocks = [], id=0
  for (let i=0; i<blocks.length; i++) {
    if (hasSpeaker(blocks[i].text))  {
      const splits = blocks[i].text.split(':')
      newBlocks.push({id:id++, text:splits[0].trim(), type:'speaker', topicExtraction:false, pageNo:blocks[i].pageNo})
      newBlocks.push({id:id++, text:splits[1].trim(), type:'paragraph', topicExtraction:splits[0].toLowerCase().trim()=='moderator'?false:true,  pageNo:blocks[i].pageNo})
    }
    else {
      newBlocks.push({id:id++, text:blocks[i].text, type:'paragraph', topicExtraction:true})
    }
  }

  return newBlocks
}

const getBlocks = async (url, type=1, newsDt) => {
  return new Promise(async (resolve, reject)=>{
    try {
      const loadingTask = type==2?pdfjsLib.getDocument({data:url}):pdfjsLib.getDocument(url)
      const pdf = await loadingTask.promise
      let blocks = [], textItems = [];

      for (let pageNo=2; pageNo<=pdf.numPages; pageNo++) {
        const page = await pdf.getPage(pageNo)
        const textContent = await page.getTextContent()

        textContent.items.forEach((item,i)=>{
          const transform = item.transform;
          const x = transform[4];
          const y = transform[5];
          const width = item.width;
          const height = item.height;
          textItems.push({text:item.str, cx:x+width/2, cy:y+height/2, width:width, height:height, x1:x, y1:y, x2:x+width, y2:y+height, pageNo:pageNo})
        })
      }

      textItems = attachRepetition(textItems)
      textItems = textItems.filter(d=>d.repetition==0 || !canDelete(d, pdf.numPages))

      for (let pageNo=2; pageNo<=pdf.numPages; pageNo++) {
        const textContent = textItems.filter(d=>d.pageNo==pageNo)
        const result = clusterText(textContent)

        blocks.push(...result.filter(d=>d.text.length>0).map(d=>({...d, pageNo:pageNo})))
      }

      let speakers = []
      blocks.filter(d=>hasSpeaker(d.text)).forEach(d=>{
        const speaker = d.text.split(':')[0]
        if (!speakers.find(d=>d.toLowerCase()===speaker.toLowerCase()))
          speakers.push(speaker)
      })
      const titlePeriod = getTitlePeriod(blocks, newsDt)
      const createdOn = getCreatedOn(blocks, newsDt)

      //look at page transitions closely and merge blocks if new page doesn't start with a speaker
      blocks = mergePageTransitionBlocks(blocks, speakers)
      blocks = markForExtraction(blocks)

      //extract topics and then separate out the authors
      //separate out authors at the start of blocks
      resolve({blocks:blocks.filter(block=>block.text.length!=0), totalPages:pdf.numPages, createdOn:createdOn, titlePeriod:titlePeriod})
    }
    catch (e) {
      //console.log(e)
      reject('link is too old')
    }
  })
}

const getPdf = async (fileName, type=1, newsDt) =>  {
  let url = `https://www.bseindia.com/xml-data/corpfiling/AttachHis/${fileName}`
  return new Promise(async (resolve, reject)=>{
    try {
      let {blocks, totalPages, createdOn, titlePeriod} = await getBlocks(type==1?url:fileName, type, newsDt)
      if (totalPages<=2) {
        url = getLinkFromPdf(blocks)
        const op = await getBlocks(url, 1, newsDt)
        blocks = op.blocks
        totalPages = op.totalPages
        createdOn = op.createdOn
        titlePeriod = op.titlePeriod
      }
      console.log(blocks)
      resolve([blocks, titlePeriod, createdOn, url])
    }
    catch (e) {
      reject (e)
    }

  })
}

function hasData(text)  {
  const numberRe = new RegExp('[0-9]+')
  return numberRe.exec(text)?true:false
}

function camelcase(topic) {
  return topic.split(' ').map(d=>d[0].toUpperCase()+d.slice(1)).join('')
}

const toPmBlock = ({text, type='paragraph', topics, titleTopics, blockGroup, blockOffset, createdBy, createdOn}) =>  {
  let attrs = {key:uniqueId()}
  if (type==='nHeading')  Object.assign(attrs,{level:1})
  let content = []
  topics.forEach((d,i)=>{
    content.push({type:'mentionAtom', attrs:{createdByHandle:'', id:i, type:'topic', showIcon:'', label:d, name:'topic'}})
    content.push({type:'text', text:' '})
  })
  content.push({type:'text', text:text})
  const textWithTopics = topics.join(' ')+' '+text
  let date = new Date()

  return {
   blockGroup:blockGroup,
   blockOffset:blockOffset,
   allTopics: [...titleTopics, ...topics],
   blockContent: {
     type:type,
     attrs: attrs,
     content: content.slice()
   },
   blockData: {type: 'note'},
   hasData: hasData(text),
   levelOneTopics:[],
   levelThreeTopics:[],
   levelTwoTopics:[],
   levelZeroTopics: titleTopics.slice(),
   text: textWithTopics,
   updatedAt: date,
   userAndBlockgroup: 'IndiaTranscripts'+blockGroup,
   nodeOnlyTopics:topics.slice(),
   createdByHandle:'IndiaTranscripts',
   processing: false,
   createdBy:createdBy,
   createdAt:createdOn,
   lastSaveBy:createdBy
  }
}

const bseToNse = async (scrip) => {
  const data = JSON.parse(await fs.readFile('./tickers.json'))
  return new Promise((resolve, reject)=>{
    const match = data.find(d=>d.bseId===scrip.toString())
    if (match)  resolve(match['nseId']+"_NS")
    resolve(scrip+"_BS")
  })
}

const conference_call = "(earnings|results)\\s*conference\\s*call\\s*(transcript)?"
const year = "(?<year>(\\d{4}|\\d{2})|((?<fcy>(f|c)y)(???|-|')?\\s*(\\d{4}|\\d{2})))" //numbers or fy or cy followed by ' - \\s
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

function getTranscriptDate(date_string) {
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
  }
  else
    return null
}

function getTitlePeriod(blocks, newsDt) {
  let titlePeriod = getLastQuarter(newsDt)

  for (let i=0; i<blocks.length; i++)  {
    let dtPeriod = getTranscriptDate(blocks[i.text])
    if (!!dtPeriod) titlePeriod = dtPeriod
  }

  return titlePeriod
}

function getCreatedOn(blocks, newsDt) {
  const re = new RegExp('mmmm_dd_yyyy_string','ig')
  let createdOn = []//store all possible matches and their frequencies

  for (let i=0; i<blocks.length; i++)  {
    const block = blocks[i]
    const match = re.exec(block.text)
    if (match)  {
      const dt = new Date(Date.parse(match.groups.date))
      const existingIdx = createdOn.findIndex(d=>d.dt===dt)
      if (existingIdx==-1) createdOn.push({dt:dt, frq:1})
      else createdOn[existingIdx].frq=createdOn[existingIdx].frq+1
    }
  }

  if (createdOn.length>0) return createdOn.sort((a,b)=>a.frq-b.frq).pop().dt
  return newsDt
}


function getSourceBlock(blockGroup, titleTopics, createdBy, createdOn, url)  {
    return {
     blockGroup:blockGroup,
     blockOffset:1,
     allTopics: titleTopics.slice(),
     blockContent: {
       type: "paragraph",
       attrs: {key: uniqueId()},
       content: [
         {
           type: "text",
           marks: [
             {
               type: "nLink",
               attrs: {
                 'data-fileandlink': [
                   {
                     type: "link",
                     location: url,
                   }
                 ],
                 "data-subtext": "Link"
               }
             }
           ],
           text: "Source file"
         }
       ]
     },
     blockData: {type: 'note'},
     hasData: false,
     levelOneTopics:[],
     levelThreeTopics:[],
     levelTwoTopics:[],
     levelZeroTopics: titleTopics.slice(),
     text: "Source file",
     updatedAt: new Date(),
     userAndBlockgroup: 'IndiaTranscripts'+blockGroup,
     nodeOnlyTopics:[],
     createdByHandle:'IndiaTranscripts',
     processing: false,
     createdBy:createdBy,
     createdAt:createdOn,
     lastSaveBy:createdBy
    }
}

const processOneTranscript = async (blocks, titlePeriod, createdOn, url, blockGroup, ticker) =>  {

  return new Promise(async (resolve, reject)=>{
    if (blocks.length==0) reject('none');
    let titleTopics = [ticker, titlePeriod, 'EarningsCallTranscript'].filter(d=>d.length>0).map(d=>camelcase(d))

    const db = mongo.db('nwb')
    const blocksDb = db.collection('blocks')
    const user = (await db.collection('users').find({username:'IndiaTranscripts'}).project({_id:1}).toArray())[0]['_id']
    const date = new Date()
    const response = await axios.post('http://127.0.0.1:5000/getTopics',{blocks:blocks.filter(block=>block['topicExtraction'])})
    const blocksWithKeywords = response.data
    let pmBlocks = blocks.map((block, i)=>{
      const match = blocksWithKeywords.find(_d=>_d.id==block.id)
      let topics = [], type = 'paragraph'

      if (match)  {
        topics=match.topics.filter(d=>d.length>0).map(d=>camelcase(d[0]))
      }
      if (block.type==='speaker') type='nHeading'

      return toPmBlock({
                        text:block.text,
                        type:type,
                        topics:topics,
                        titleTopics:titleTopics,
                        blockOffset:i+2,  //reserved for source block and title block
                        blockGroup:blockGroup,
                        createdBy:user,
                        createdAt:createdOn
                      })
    })

    pmBlocks.unshift(getSourceBlock(blockGroup, titleTopics, user, createdOn, url))

    let titleContent = []
    titleTopics.forEach((d,i)=>{
      let showIcon = ''
      if (i==0) showIcon='????'
      titleContent.push({type:'mentionAtom', attrs:{createdByHandle:'', id:i, type:'topic', showIcon:i==0?'????':'', label:d, name:'topic'}})
      titleContent.push({type:'text', text:' '})
    })

    pmBlocks.unshift({
      blockGroup:blockGroup,
      blockOffset:0,
      allTopics: titleTopics,
      blockContent: {
        type:'title',
        attrs: {key:uniqueId(), createdOn:date.getTime(), createdBy:'IndiaTranscripts'},
        content: titleContent
      },
      blockData: {type: 'note'},
      hasData: false,
      levelOneTopics:[],
      levelThreeTopics:[],
      levelTwoTopics:[],
      levelZeroTopics: titleTopics.slice(),
      text: titleTopics.join(' '),
      updatedAt: date,
      userAndBlockgroup: 'IndiaTranscripts'+blockGroup,
      nodeOnlyTopics:titleTopics,
      createdByHandle:'IndiaTranscripts',
      processing: false,
      createdBy:user,
      createdAt:createdOn,
      lastSaveBy:user
    })
    blocksDb.insertMany(pmBlocks).then((response)=>console.log(response))
    resolve('done')
  })

  //fs.writeFile('transcriptWithKeywords.json', JSON.stringify(blocks), 'utf8', (err, done)=>console.log('done'))

}



const processAllTranscripts = async () =>  {
    await mongo.connect()
    const db = mongo.db('nwb')
    const blocks = db.collection('blocks')
    const user = (await db.collection('users').find({username:'IndiaTranscripts'}).project({_id:1}).toArray())[0]['_id']

    const processedJobs = db.collection('processedjobs')
    let blockGroup = await blocks.find({createdByHandle:'IndiaTranscripts'}).sort({blockGroup:-1}).limit(1).project({blockGroup:1, _id:0}).toArray()

    if (blockGroup.length==0) blockGroup = 1
    else blockGroup=blockGroup[0].blockGroup+1

    const re = new RegExp('transcript','ig')
    const files = await fs.readdir('./announcements')
    const transcriptLinks = []
    for (let i=0; i<files.length; i++)  {

      const file = files[i]
      const data = JSON.parse(await fs.readFile(`./announcements/${file}`, 'utf8'))
      for (let j=0; j<data.length; j++) {
        const item = data[j]
        if (re.exec(item.HEADLINE)) {
          const newsDate = new Date(item.NEWS_DT)
          const scrip = item.SCRIP_CD
          const nse_id = await bseToNse(scrip)
          const isDone = await processedJobs.find({type:'IndiaTranscripts', 'details.scrip':nse_id, 'details.newsDate':newsDate}).toArray()
          if (isDone.length==0)  {
            console.log("Processing script "+scrip+' with attachment '+item.ATTACHMENTNAME+' in file '+file)
            try {
                await processOneTranscript(...await getPdf(item.ATTACHMENTNAME, 1, newsDate), blockGroup++, nse_id)
            }
            catch (e) {
              console.log(e)
            }
            finally {
              await processedJobs.insertOne({type:'IndiaTranscripts', details:{scrip:nse_id, newsDate:newsDate}})
            }
          }

          console.log('finished')
        }
      }
    }
}



async function _processOneTranscript(file, blockGroup, ticker) {
  await mongo.connect()
  const db = mongo.db('nwb')
  const user = (await db.collection('users').find({username:'IndiaTranscripts'}).project({_id:1}).toArray())[0]['_id']
  const data = await fs.readFile(file)
  try {
    processOneTranscript(...await getPdf(data, 2, new Date()), blockGroup, 'ABB_NS')
  } catch (e) {
    console.log(e)
  }

}

processAllTranscripts()
//_processOneTranscript('./transcripts/d9df70d1-74ee-4e17-9ce3-f82a5046feb3.pdf', 2)
//_processOneTranscript('./transcripts/benaras_beads.pdf', 667)
