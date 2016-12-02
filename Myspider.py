import re
from bs4 import BeautifulSoup
from urllib import request
import chardet
import os
import gzip
import threading
import _thread
from time import sleep
import random
import socket
import urllib
"""
	这是一个基于urllib实现全网爬取的多线程爬虫
	Create by Jack
"""

#想要抓取的关键字列表
keywords = ['人工智能','AI','ai']

#入口url列表
urls = [
		'http://www.ailab.cn/news/ainew/',
		'http://news.chinaso.com/search?wd='+request.quote('人工智能+行业'),
		'https://www.baidu.com/s?ie=utf-8&cus_sid=18364444163281065044&tn=SE_pscse_053x7tyx&wd=%E4%BA%BA%E5%B7%A5%E6%99%BA%E8%83%BD%20%E8%A1%8C%E4%B8%9A'
		'http://search.ifeng.com/sofeng/search.action?q=%E4%BA%BA%E5%B7%A5%E6%99%BA%E8%83%BD+%E8%A1%8C%E4%B8%9A&c=1&chel='
		'http://search.southcn.com/web/search?channelid=216505&&searchword=%E4%BA%BA%E5%B7%A5%E6%99%BA%E8%83%BD%20%E8%A1%8C%E4%B8%9A',
		'https://www.baidu.com/s?ie=utf-8&cus_f=0&wd=%E4%BA%BA%E5%B7%A5%E6%99%BA%E8%83%BD%01%E8%A1%8C%E4%B8%9A&tn=71032170_cpr&rsv_lu=3_sb&si=china.com&ct=2097152&fenlei=mv6qUAdxTZPxTvb0IZRqIHDLnjmdnWc0T1YYPARYrHN-nAR4uhD1uHTL0AGo5HmLPH7hP1u9PHD4Pj9bnhm0IAYqnWm3PjfkPHRz0APh5Hn0Thcqn0K_IyVG5HD0mv4YUWYLnH01nWDLn7qWTZc0TLPs5HD0TLPsnWYk0ZwYTjYk0A4YIZ0qnfKLpHYY0Au1mv9VujYz0Zwb5HDLnjmdnWc0IgF_5y9YIZ0-nYD-nbm-nbuYuyPCFHF7mv9GUhD-nbNWUvY-nbm0',
		'http://search.sina.com.cn/?q=%C8%CB%B9%A4%D6%C7%C4%DC+%D0%D0%D2%B5&range=all&c=news&sort=rel',
		'http://sou.chinanews.com.cn/search.do?q=%E4%BA%BA%E5%B7%A5%E6%99%BA%E8%83%BD+%E8%A1%8C%E4%B8%9A&ps=10&time_scope=0&channel=all&sort=_score',
		'http://news.sogou.com/news?query=%C8%CB%B9%A4%D6%C7%C4%DC+%D0%D0%D2%B5',
		'https://www.sogou.com/sogou?site=news.qq.com&query=%C8%CB%B9%A4%D6%C7%C4%DC+%D0%D0%D2%B5&pid=sogou-wsse-b58ac8403eb9cf17-0004&sourceid=&idx=f&idx=f'
]

#用一个递增的数字作为文件名标识
counter = 0
#互斥锁，用于线程对互斥资源的访问
mutex = threading.Lock()

"""
	@param: values	正文段列表
			k 		每次选取的段数
	@return:left	正文起始段
			right	正文结束段
"""
def findEnd(values,k=5):
	left,right = 0,0
	# print(values)
	for i in range(len(values)):
		strCnt = sum(values[i:i+k])
	# print(strCnt)
		if strCnt > 180 and values[i] > 15:
			if left == 0:
				left = i
			right = i
	cnt = 0
	for i in range(right,len(values)):
		if cnt >= k:
			break
		if values[i] > 15:
			right += 1
		cnt += 1
	return left,right

"""
找出正文的起始位置和结束位置
	@param: content	过滤标签后的网页
			k 		每段选取的行数
	@return:left	正文起始段
			right	正文结束段
"""
def filt(content,k=1):
	if not content:
		return None,None
	lines = content.split('\n')
	group_value = []
	for i in range(0,len(lines),k):
		group = '\n'.join(lines[i:i+k]).strip()
		group_value.append(len(group))
	left,right = findEnd (group_value)
	return left,right

"""
基于正文提取算法（行快分布）实现的提取函数
	@param: content	过滤标签后的网页
	@return:res		正文
"""
def extract (content):
	if content.count('\n') < 10:                #如果换行符少于10，则认为html是经过压缩的，进行换行处理
		content = content.replace('>\n','\n')
	content = tool.replace(content)
	left,right = filt(content)
	return '\n'.join(content.split('\n')[left:right])

"""
为特殊网页结构而特别设计的提取函数
	@param: content	过滤标签后的网页
	@return:res		正文
"""
def extractForAilab(content):
	try:
		# tool.save(''.join(content.split('\n'))+'\n\n\n','a','haha')
		soup = BeautifulSoup(content,'html.parser')
		main = soup.find(attrs={'id':'mainDiv'})
		[s.extract() for s in main(['div'])]
		s = main.get_text()
		r = re.compile(r'<[^>]+>',re.M|re.S)
		s = r.sub('',s).strip()                                     #删除HTML标签
		r = re.compile(r'^\s+$', re.M|re.S)                     #删除空白行
		s = r.sub ('', s)
		r = re.compile(r'\n+',re.M|re.S)                        #合并空行为一行
		s = r.sub('\n',s)
		return s
	except Exception as e:
		print(e)
		return ''

"""
子线程抓取页面，并以文本文件的方式将其存到本地磁盘中
	@param: url 	当前要抓取的页面
"""
def loop(url):
	try:
		global counter,mutex
		print('fetching %s...' %url)
		page = tool.fetchPage(url)
		if page =='':
			print('drop:%s'%url)
			tool.save(url+'\n','a','dropedUrls')
			return
		r = re.compile(r'<title>(.*?)</title>',re.M|re.S)
		title = ''.join(re.findall(r,page))
		if spider.getHost(url) == 'http://www.ailab.cn':
			body = extractForAilab(page)
		else:
			body = extract(page)
		if body == '':
			print('drop:%s'%url)
			tool.save(url+'\n','a','dropedUrls')
		else:
			if mutex.acquire():		#互斥锁
				counter+=1
				tool.save('标题：'+title+'\n\n'+body,name=str(counter))
				mutex.release()
	except:
		print('loop error')
		return ''


class MySpider:
	def __init__(self,urls):
		self.cur_depth = 0
		self.queue = MyQueue()
		tool.remove('urls')
		tool.remove('important')
		tool.remove('dropedUrls')
		self.url_dict = dict()
		if isinstance(urls,str):
			self.queue.addUnvisitedUrl(urls,1)
			self.url_dict[url] = 1
		elif isinstance(urls,list):
			for url in urls:
				self.queue.addUnvisitedUrl(url,1)
				self.url_dict[url] = 1

"""
通过bfs进行搜索爬取感兴趣的页面
	@param: depth 	爬取的深度
"""
	def crawl(self,depth):
		queue = self.queue
		for i in range(depth):
			unvisted_links = []
			while not queue.isUnvisitedEmpty():
				try:
					cur_url = queue.unVisitedDequeue()
					print('pop link:%s' % cur_url)
					links = []
					if cur_url is not None and cur_url != '':
						links = self.fetchLinks(cur_url)
					unvisted_links.extend(links)
					print('get %d new links'%len(links))
					queue.addVisitedUrl(cur_url)
					print('visited count:%d' % queue.getVisitedNums())
					print('current depth:%d' % (i+1))
				except:
					print('crawl error')
					continue
			if len(unvisted_links) > 0:
				for link in unvisted_links:
					level = self.url_dict[link]
					queue.addUnvisitedUrl(link,level)
			else:
				break
			print('unVisited count:%d' % queue.getunVisitedNums())

"""
解析当前页面中的全部url连接，抓取感兴趣的页面，并将过滤后的url列表加入待爬取队列中
这里用了一个简单的思想：1. 如果当前页面中的子页面是我们感兴趣的，则认为这个页面是重要的
						2. 如果当前页面中我们感兴趣的子页面超过10个，则认为这个页面也是重要的
	@param: depth 	爬取的深度
"""
	def fetchLinks(self,url):
		links = []
		page = tool.fetchPage(url)
		page_num = 0
		url_dict = self.url_dict			
		if page != '':
			try:
				soup = BeautifulSoup(page,'html.parser')
				items = soup.findAll('a')
				for item in items:
					link = item.get('href')
					title = item.get_text()
					newlink = ''					
					pageType = 0
					if link != None and link!='' and len(link) > 3 and 'javascript' not in link:
						for k in keywords:
							if k in title:
								pageType = 1
								break
						if link[:4] != 'http':
							host = self.getHost(url)
							if link[0] in ['/','?']:
								newlink = host + link
							else:
								newlink = host + '/' + link
						elif link[:4] == 'http':
							newlink = link
						if link!='':
							if newlink not in url_dict:
								links.append(newlink)
								tool.save(link+'\n',option='a',name='urls')
								if  pageType == 1:
									_thread.start_new_thread(loop,(newlink,))
									page_num += 1
			except:
				print('extract error')
				return []
		
		if page_num >= 10:		#当当前页面发现十个以上重要的子页面时，认为该页面也是重要的，并将所有子页面标记为重要
			tool.save(url+'\n','a',name="important")
			url_dict[url] = 1
		else:
			url_dict[url] = 0
		level = url_dict[url]
		for link in links:
			url_dict[link] = level
		return links
"""
获取url中的域名
	@param: url
"""
	def getHost(self,url):
		r = re.compile(r'(https?://)?[^/\s]*',re.S)
		g = r.match(url)
		host = ''.join(g.group())
		return host
"""
判断url是否为html类型的
	@param: url
"""
	def judgePage(self,url):
		r = re.compile('[xs]?htm[l]?$')
		s = re.findall(r,url)
		return len(s) == 1

"""
一个模拟队列的数据结构，这里做了个优化：
通过使用两个unVisited列表（first，second）实现多级队列，从而使感兴趣的内容能够尽快被访问到，而不是被其他无用的内容抢先
"""
class MyQueue:
	def __init__(self):
		self.visited = []
		self.unVisitedFirst = []	#采用多级队列实现优先级分级
		self.unVisitedSecond = []

	def getVisitedUrls(self):
		return self.visited

	def getUnvisitedUrls(self):
		return self.unVisitedFirst.extend(self.unVisitedSecond)

	def addVisitedUrl(self,url):
		self.visited.append(url)

	def removeVisitedUrl(self,url):
		self.visited.remove(url)
	#将最后一个元素（即队列头）移出列表
	def unVisitedDequeue(self):
		try:
			if len(self.unVisitedFirst) > 0:	#只有当一级队列为空时，才返回二级队列的内容
				return self.unVisitedFirst.pop()
			else:
				return self.unVisitedSecond.pop()
		except:
			return None

	#将新加入的url插入到列表头（即对列尾），先进先出	
	def addUnvisitedUrl(self,url,level):	
		if url!='' :
			if level == 1:
				self.unVisitedFirst.insert(0,url)
			else:
				self.unVisitedSecond.insert(0,url)
	def getVisitedNums(self):
		return len(self.visited)
	def getunVisitedNums(self):
		return len(self.unVisitedFirst) + len(self.unVisitedSecond)
	def isUnvisitedEmpty(self):
		return len(self.unVisitedFirst)==0 and len(self.unVisitedSecond) == 0

"""
类如其名，一个封装了各种操作的工具类
包括：	1.html页面标签过滤
		2.解压gzip压缩的页面
		3.保存文件到磁盘中
		4.删除特定目录下的文件
		5.通过url抓取页面，经过各种处理后，返回字符串
"""
class Tool:
	path = os.getcwd()+os.sep +'news'+os.sep
	headers = {
		'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
		'Referer':'http://www.google.com',
		}

	def replace(self,content):
		r = re.compile(r'<script.*?</script>',re.I|re.M|re.S)   #删除JavaScript
		s = r.sub ('',content)
		r = re.compile(r'<style.*?</style>',re.I|re.M|re.S)     #删除css
		s = r.sub ('', s)
		r = re.compile(r'<!--.*?-->', re.I|re.M|re.S)           #删除注释
		s = r.sub('',s)
		r = re.compile(r'<meta.*?>', re.I|re.M|re.S)            #删除meta
		s = r.sub('',s)
		r = re.compile(r'<ins.*?</ins>', re.I|re.M|re.S)        #删除ins
		s = r.sub('',s)
		r = re.compile(r'<[^>]+>',re.M|re.S)
		s = r.sub('',s).strip()                                     #删除HTML标签
		r = re.compile(r'^\s+$', re.M|re.S)                     #删除空白行
		s = r.sub ('', s)
		r = re.compile(r'\n+',re.M|re.S)                        #合并空行为一行
		s = r.sub('\n',s)
		r = re.compile('\s+',re.I|re.M|re.S)
		s = r.sub('\n',s)
		return s

	#解压gzip压缩的网页
	def gzdecode(self,data):
		charset = chardet.detect(data)['encoding']
		if charset == None:
			charset = 'utf-8'
		if charset.lower() == 'gb2312':
			charset = 'gb18030'
		try:
			html = gzip.decompress(data).decode(charset)
		except OSError:
			html = data.decode(charset)
		return html

	def save(self,data,option='w',name='out'):
		path = self.path
		if os.path.exists(path) == False:
			os.makedirs(path)
		f = open(path+name+'.txt',option,encoding='utf-8')
		f.write(data)
		f.close()

	def remove(self,name):
		path = self.path+name+'.txt'
		if os.path.exists(path):
			print('remove...')
			os.remove(path)
		else:
			return

	def fetchPage(self,url):
		page = ''
		for i in range(3):
			try:
				socket.setdefaulttimeout(5)
				req = request.Request(url,headers=self.headers)
				page = request.urlopen(req).read()
				page = self.gzdecode(page)
				# print('success')
				return page
			except request.HTTPError as e:
				print(e.reason)
				return ''
			except request.URLError as e:
				print(e.reason)
				return ''
			except socket.timeout as e:
				if i < 2:
					continue
				else:
					print('timeout')
					return ''
			except:
				print('other error')
				return ''

if __name__ == '__main__':
	tool = Tool()
	spider = MySpider(urls)
	spider.crawl(10)
