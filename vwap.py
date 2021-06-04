import cbpro, time
from csv import writer
# This function is to write the data into csv file
def writ2eCSV(file_name, list_of_elem):
    # Open file in append mode
    with open(file_name, 'a+', newline='') as write_obj:
        # Create a writer object from csv module
        csv_writer = writer(write_obj)
        # Add contents of list as last row in the csv file
        csv_writer.writerow(list_of_elem)

# The class to calculate eh vwap.
class VWAP:
    def __init__(self, name):
        self.name = name
        self.historySizeList = []
        self.maxDataPoints =200
        self.numOfRecords=0
        self.historyAmountList = []
        self.totalAmount=0.0
        self.totalSize = 0.0
        self.currentVWAP = 0.0

    def add_data(self, matchMsg):
        if self.numOfRecords<= self.maxDataPoints:
            self.totalSize += float(matchMsg.get('size'))
            self.totalAmount += float(matchMsg.get('size'))*float(matchMsg.get('price'))
            self.numOfRecords+=1
        else:
            # Remove the first record size in the total size at the same time pop the first size in the list
            self.totalSize += (float(matchMsg.get('size'))-self.historySizeList.pop(0))
            # Remove the first Amount size in the total size at the same time pop the first amount in the list
            self.totalAmount += (float(matchMsg.get('size'))*float(matchMsg.get('price'))-self.historyAmountList.pop(0))

        self.currentVWAP = self.totalAmount/self.totalSize
        self.historySizeList.append(float(matchMsg.get('size')))
        self.historyAmountList.append(float(matchMsg.get('size'))*float(matchMsg.get('price')))

        writ2eCSV(self.name+'.csv', [matchMsg.get('product_id'),matchMsg.get('time'), self.currentVWAP])

    def getvwap (self):
        return  self.currentVWAP


BTC2USD = VWAP('BTC-USD')
ETH2USD = VWAP('ETH-USD')
ETH2BTC = VWAP('ETH-BTC')

class myWebsocketClient(cbpro.WebsocketClient):
    def on_open(self):
        self.url = "wss://ws-feed.pro.coinbase.com/"
        self.products = ["BTC-USD","ETH-USD","ETH-BTC"]
        self.message_count = 0
        self.channels =  [{ "name": "matches"}]

    def on_message(self, msg):
        self.message_count += 1
        if msg.get("product_id") == "BTC-USD":
            BTC2USD.add_data(msg)
        if msg.get("product_id") == "ETH-USD":
            ETH2USD.add_data(msg)
        if msg.get("product_id") == "ETH-BTC":
            ETH2BTC.add_data(msg)

    def on_close(self):
        print("-- Goodbye! --")

wsClient = myWebsocketClient()
wsClient.start()
print(wsClient.url, wsClient.products)
# Get 5000
while (wsClient.message_count < 5000):
    print ("\nmessage_count =", "{} \n".format(wsClient.message_count))
    time.sleep(1)
wsClient.close()