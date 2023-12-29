__author__ = 'ketankk'


import ftplib
server = ftplib.FTP()
server.connect('mbkext-sftp.mobikwik.com', 22)
server.login('credgenics','Credgenics@mbk23')
# You don't have to print this, because this command itself prints dir contents
server.dir()

'''
Endpoint => mbkext-sftp.mobikwik.com
Username => credgenics
Password => Credgenics@mbk23
Path => /mbk-client-sftp-prod/credgenics


'''


import pysftp
host = "mbkext-sftp.mobikwik.com"
username = "credgenics"
password = "Credgenics@mbk23"


with pysftp.Connection(host, username=username, password=password) as sftp:
    sftp.listdir()
    #sftp.put(localpath, path)


#Shell command
sftp credgenics@mbkext-sftp.mobikwik.com
Credgenics@mbk23