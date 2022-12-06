import {promises as fs} from 'fs'
import pdfjsLib from 'pdfjs-dist'
import clustering from 'density-clustering'
import levenshtein from 'fast-levenshtein'

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
  return {
    cx:cx, cy:cy, width:w, height:h, x1:x1, y1:y1, x2:x2, y2:y2
  }
}

const clusterText = (textContent) =>  {
  let optics = new clustering.OPTICS();

  const lineClusters = optics.run(textContent, 15, 2, getDist)
  let results = [], boundingBox = {}
  lineClusters.forEach(cluster=>{
    let cum='', bbox={}
    cluster.forEach((i,idx)=>{
      cum=idx==0?textContent[i].text:cum+' '+textContent[i].text
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
  const re = new RegExp(/:/ig)  //this most likely indicates a moderator
  return !re.exec(item.text) && item.repetition>Math.floor(0.75*totalPages)
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
  let newBlocks = []
  for (let i=0; i<blocks.length; i++) {
    if (hasSpeaker(blocks[i].text))  {
      const splits = blocks[i].text.split(':')
      newBlocks.push({text:splits[0].trim(), type:'speaker', topicExtraction:false})
      newBlocks.push({text:splits[1].trim(), type:'paragraph', topicExtraction:splits[0].toLowerCase().trim()=='moderator'?false:true})
    }
    else {
      newBlocks.push({text:blocks[i].text, type:'paragraph', topicExtraction:true})
    }
  }

  return newBlocks
}

const getBlocks = async (url, type=1) => {
  return new Promise(async (resolve, reject)=>{
    //const loadingTask = pdfjsLib.getDocument({data:url} //from file
    try {
      const loadingTask = type==2?pdfjsLib.getDocument({data:url}):pdfjsLib.getDocument(url)
      const pdf = await loadingTask.promise
      let blocks = [], textItems = [];

      for (let pageNo=1; pageNo<pdf.numPages; pageNo++) {
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

      for (let pageNo=2; pageNo<pdf.numPages; pageNo++) {
        const textContent = textItems.filter(d=>d.pageNo==pageNo)
        const result = clusterText(textContent)

        blocks.push(...result.filter(d=>d.text.length>0).map(d=>({...d, page:pageNo})))
      }

      let speakers = []
      blocks.filter(d=>hasSpeaker(d.text)).forEach(d=>{
        const speaker = d.text.split(':')[0]
        if (!speakers.find(d=>d.toLowerCase()===speaker.toLowerCase()))
          speakers.push(speaker)
      })


      //look at page transitions closely and merge blocks if new page doesn't start with a speaker
      blocks = mergePageTransitionBlocks(blocks, speakers)
      //blocks = markForExtraction(blocks)

      //extract topics and then separate out the authors
      //separate out authors at the start of blocks


      //removeRepetitiveText(pages)
      resolve({blocks:blocks, totalPages:pdf.numPages})
    }
    catch (e) {
      console.log(e)
      console.log('link is too old')
      resolve({blocks:[], totalPages:3})
    }
  })
}

const getPdf = async (fileName, type=1) =>  {
  let url = `https://www.bseindia.com/xml-data/corpfiling/AttachHis/${fileName}`

  return new Promise(async (resolve, reject)=>{
    let {blocks, totalPages} = await getBlocks(type==2?fileName:url, type)
    if (totalPages<=2) {
      url = getLinkFromPdf(blocks)
      const op = await getBlocks(newUrl)
      blocks = op.blocks
      totalPages = op.totalPages
    }
    resolve(blocks)
  })
}

const getTranscriptLinks = async () =>  {
    const re = new RegExp('transcript','ig')
    const files = await fs.readdir('./announcements')
    const transcriptLinks = []
    for (let i=0; i<files.length; i++)  {
      const file = files[i]
      const data = JSON.parse(await fs.readFile(`./announcements/${file}`, 'utf8'))
      for (let j=0; j<data.length; j++) {
        const item = data[j]
        if (re.exec(item.HEADLINE)) {
          const newDate = new Date(item.NEWS_DT)
          const scrip = item.SCRIP_CD
          await getPdf(item.ATTACHMENTNAME)
        }
      }
    }
}

const processOneTranscript = async (file) =>  {
  const data = await fs.readFile(file)
  const loadingTask = pdfjsLib.getDocument({data:data})
  const pdf = await loadingTask.promise
  //console.log(pdf.numPages)
  const blocks = await getPdf(data, 2)
  console.log(blocks)
}

//getTranscriptLinks()
processOneTranscript('./transcripts/d9df70d1-74ee-4e17-9ce3-f82a5046feb3.pdf')
