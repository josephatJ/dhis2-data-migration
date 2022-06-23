class DHIS2PersonData:
  def __init__(self, username, password, url):
    self.username = username
    self.password = password
    self.url = url

  def get_client_details(self):
    print("Hello my name is " + self.username)