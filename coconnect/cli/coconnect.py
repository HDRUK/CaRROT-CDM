import requests

url = "http://127.0.0.1:8080/scanreports/1/mapping_rules"

headers = {'Content-Length': '97', 'Content-Type': 'application/x-www-form-urlencoded', 'Host': '127.0.0.1:8080', 'Connection': 'keep-alive', 'Cache-Control': 'max-age=0', 'Upgrade-Insecure-Requests': '1', 'Origin': 'http://127.0.0.1:8080', 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_1_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36', 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9', 'Sec-Fetch-Site': 'same-origin', 'Sec-Fetch-Mode': 'navigate', 'Sec-Fetch-User': '?1', 'Sec-Fetch-Dest': 'document', 'Referer': 'http://127.0.0.1:8080/scanreports/1/mapping_rules', 'Accept-Encoding': 'gzip, deflate, br', 'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8,fr;q=0.7', 'Cookie': 'csrftoken=w0YAXyrQpBsRwpN6t0WwwCRBu31NMWatoA3V9njuLBWgUaL1HFKa0dkxYuUFtaO8; sessionid=py2t38hvst0pynsciusu4z7n1f7o3zjj'}

response = requests.post(url,headers=headers)

print (response)
print (response.url)
