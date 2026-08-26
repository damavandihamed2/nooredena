import requests

headers = {
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9,fa;q=0.8',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Origin': 'https://tts.ime.co.ir',
    'Pragma': 'no-cache',
    'Referer': 'https://tts.ime.co.ir/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}

response = requests.get('https://dataapi.ime.co.ir/api/spotmarketdata/GetMarketsInfo', headers=headers)



"""
[

  {
    "MarketId": 1,
    "MarketName": "صنعتی و معدنی",
    "StartTime": "12:30:00",
    "FinishTime": "13:11:00",
    "Duration": 0,
    "Counter": 0,
    "Color": "",
    "StepDescription": "",
    "Activate": null,
    "Count": null,
    "Status": 3
  },
  {
    "MarketId": 1,
    "MarketName": "صنعتی و معدنی",
    "StartTime": "12:30:00",
    "FinishTime": "14:02:00",
    "Duration": 0,
    "Counter": 0,
    "Color": "",
    "StepDescription": "",
    "Activate": null,
    "Count": null,
    "Status": 4 --
  },




  {
    "MarketId": 3,
    "MarketName": "پتروشیمی و فرآورده های نفتی",
    "StartTime": "13:30:00",
    "FinishTime": "14:00:00",
    "Duration": 0,
    "Counter": 0,
    "Color": "",
    "StepDescription": "",
    "Activate": null,
    "Count": null,
    "Status": 3
  },
  {
    "MarketId": 3,
    "MarketName": "پتروشیمی و فرآورده های نفتی",
    "StartTime": "13:30:00",
    "FinishTime": "15:36:00",
    "Duration": 0,
    "Counter": 0,
    "Color": "",
    "StepDescription": "",
    "Activate": null,
    "Count": null,
    "Status": 4 --
  },



   {
    "MarketId": 6,
    "MarketName": "صادراتی کیش",
    "StartTime": "11:30:00",
    "FinishTime": "11:41:34",
    "Duration": 0,
    "Counter": 0,
    "Color": "",
    "StepDescription": "",
    "Activate": null,
    "Count": null,
    "Status": 3 انجام شده
  },
  {
    "MarketId": 6,
    "MarketName": "صادراتی کیش",
    "StartTime": "11:30:00",
    "FinishTime": "12:04:30",
    "Duration": 0,
    "Counter": 0,
    "Color": "",
    "StepDescription": "",
    "Activate": null,
    "Count": null,
    "Status": 2
  },


  {
    "MarketId": 11,
    "MarketName": "فرعی",
    "StartTime": "14:00:00",
    "FinishTime": "14:33:00",
    "Duration": 0,
    "Counter": 0,
    "Color": "",
    "StepDescription": "",
    "Activate": null,
    "Count": null,
    "Status": 3
  }
  {
    "MarketId": 11,
    "MarketName": "فرعی",
    "StartTime": "14:00:00",
    "FinishTime": "15:15:00",
    "Duration": 0,
    "Counter": 0,
    "Color": "",
    "StepDescription": "",
    "Activate": null,
    "Count": null,
    "Status": 4 --
  },
 
 



  {
    "MarketId": 20,
    "MarketName": "سیمان",
    "StartTime": "12:00:00",
    "FinishTime": "13:51:00",
    "Duration": 0,
    "Counter": 0,
    "Color": "",
    "StepDescription": "",
    "Activate": null,
    "Count": null,
    "Status": 4 --
  }
]

"""








