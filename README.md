# bilibili
ASMR ...

## ABSTRACT
Get video urls which title include 'ASMR' from [www.bilibili.com/video/](http://www.bilibili.com/video/music-video-1.html).

## PLATFORM
* OS: Linux
* python: python2.7+ and MySQLdb module
* mysql: server and client

## OUTPUT
### mysql
* need MySQLdb module in python, mysql-server and mysql-client in localhost
* need get passwd for user, default user=root
* will insert result into db=video, table=asrm

### json
* will dump result into result.json
