from pire.client import PireClient

c = PireClient(input("client_id: "))
c.start()
c.run()