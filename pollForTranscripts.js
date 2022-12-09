//import fs from 'fs'
import { promises as fs } from "fs";
import axios from 'axios'

const getUrl = (pageNo, bseId, from, to) =>  {
    return `https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w?pageno=${pageNo}&strCat=Company+Update&strPrevDate=${from}&strScrip=${bseId}&strSearch=P&strToDate=${to}&strType=C`
}

const HeaderSpoof = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:107.0) Gecko/20100101 Firefox/107.0'}

const getDates = (seedYear, maxYear=new Date().getFullYear(), maxDate=new Date()) => {
  const curYear = new Date().getFullYear()
  let dates = []
  for (let year=seedYear; year<=maxYear; year++)  {
    const from = new Date(year, 0, 1,12)
    const to = year==maxYear?maxDate:new Date(year, 11, 31,12) //12 hour schedule tips it to next day
    dates.push([from.toISOString().slice(0,10).split('-').join(''),
                to.toISOString().slice(0,10).split('-').join('')])
  }
  return dates
}

const getEarningsCallTranscript = (data)  =>  {
  const re = new RegExp('transcript','ig')
  return (data.filter(d=>re.exec(d.NEWSSUB)).map(d=>({
    ATTACHMENTNAME:d.ATTACHMENTNAME,
    NEWS_DT:d.NEWS_DT,
    QUARTER_ID:d.QUARTER_ID,
    CATEGORYNAME:d.CATEGORYNAME,
    HEADLINE:d.HEADLINE,
    SCRIP_CD:d.SCRIP_CD,
    HEADLINE:d.HEADLINE,
    AGENDA_ID:d.AGENDA_ID,
    NEWSSUB:d.NEWSSUB
  })))
}

const pollForTranscripts = async () =>  {
  let pageNo=1, skip=false//, startFrom='506079',
  const data = await fs.readFile('tickers.json','utf8')
  const bseIdList = JSON.parse(data).map(d=>d.bseId)
  const dates = getDates(2015)
  //let bseIdList=[532540]

  for (let i=0; i<bseIdList.length; i++) {
    const bseId = bseIdList[i]
    //if (bseId===startFrom) skip = false
    if (!skip)  {
      let links = []
      for (let j=0; j<dates.length; j++)  {
        let pagesLeft = true

        while (pagesLeft) {
          let from=dates[j][0]
          let to=dates[j][1]
          const url = getUrl(pageNo, bseId, from, to)
          const response = await axios.get(url,{headers: HeaderSpoof})
          links.push(...getEarningsCallTranscript(response.data.Table))
          //console.log(response.data)
          if (response.data.Table.length==0)  {
            pageNo=1
            pagesLeft=false
          }
          else {
            const totalPages = response.data.Table[0].TotalPageCnt
            if (pageNo===totalPages)  {
              pageNo=1
              pagesLeft=false
            }
          }
          if (pagesLeft)  pageNo=pageNo+1
        }
      }
      
      if (links.length>0)
        fs.writeFile(`./announcements/${bseId}.json`, JSON.stringify(links), 'utf8', (err, done)=>console.log('done with '+bseId))
    }
  }
}

pollForTranscripts()
