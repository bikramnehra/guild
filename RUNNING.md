This project has 3 parts
1. data preprocessing
2. REST Server
3. web UI


1. Data Preprocessing : takes in data from stack overflow and write in hbase
To read the process for data preprocessing
Please read dataPreprocess/RUNNING.md

2. Web Application: Reads hbase and provides interactive candidate search system. Follow the steps to start the web application

	a. Place the rest/hbase-rest.py in the cluster using scp command:
	   scp hbase-rest.py userID@rcg-linux-ts1.rcg.sfu.ca:

	b. Start the thrift server using the following command:
	   hbase thrift start -threadpool

	c. Run the hbase-rest.py script:
	   python hbase-rest.py

	d. Make sure the ports are forwarded to be listened on local machine. If not then use following command to do so:
	ssh -L 50070:rcg-hadoop-01.rcg.sfu.ca:50070 -L 8088:rcg-hadoop-02.rcg.sfu.ca:8088 -L 19888:rcg-hadoop-02.rcg.sfu.ca:19888 -L 16010:rcg-hadoop-03.rcg.sfu.ca:16010 bsa61@rcg-linux-ts1.rcg.sfu.ca

	e. Open the browser in disabled security mode (this is to ensure local json files are read by browser)

	Mac: open -a Google\ Chrome --args --disable-web-security
    Windows: chrome.exe --user-data-dir="C:/Chrome dev session" --disable-web-security

    f. Now open index.html file in browser and start playing by typing tags.





