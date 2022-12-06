import fs from 'fs'
import csv from 'fast-csv'

const getBseTickers = async (nseTickers) => {
  let rows = []
  return new Promise((resolve, reject)=>{
    fs.createReadStream('tickerList.csv')
      .pipe(csv.parse())
      .on("error", err=>console.log(err))
      .on("data", row=>rows.push(row))
      .on("end", rowCount=>{
        resolve(rows.map(d=>{
          const match = nseTickers.find(_d=>_d.uid==d[7])
          return {bseId:d[0], bseName:d[3], nseId:match?match.nseId:null, nseName:match?match.name:null}
        }).filter(d=>!!d.nseId))
      })
  })
}

const getNseTickers = async () => {
  let rows = []
  return new Promise((resolve, reject)=>{
    fs.createReadStream('tickerListNse.csv')
      .pipe(csv.parse())
      .on("error", err=>console.log(err))
      .on("data", row=>rows.push(row))
      .on("end", rowCount=>{
        resolve(rows.map(d=>({nseId:d[0], name:d[1], uid:d[6]})))
      })
  })
}

const mergeTickers = async () => {
  const nseTickers = await getNseTickers()
  const mergedTickers = await getBseTickers(nseTickers)

  fs.writeFile('tickers.json', JSON.stringify(mergedTickers), 'utf8', (err, done)=>console.log(done)) 

}

mergeTickers()
