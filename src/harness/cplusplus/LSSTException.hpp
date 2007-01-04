#include <string>


using namespace std;

class LSSTException
{
public:
  LSSTException(string message);
  const char* getErrorString();
private:
  string myMessage;
};


LSSTException::LSSTException(string message)
{
  myMessage = message;
}

const char* LSSTException::getErrorString()
{

  return myMessage.c_str();
}



