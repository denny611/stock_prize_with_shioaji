import keyring


keyring.set_password("shioaji", "api_key", "JA6bkTZVNTcWFGaG2y22sTAmRmduhi1VmbHDshtWc1PY")     # @api key
keyring.set_password("shioaji", "secret_key", "4JuYfTUT2Lyk2MABr5smnY1niRW191zw5X7bWngQfmkD")  # @secret key
#keyring.set_password("shioaji", "secret_key","sino_query")  

api_key = keyring.get_password("shioaji", "api_key")     # @api key
secret_key = keyring.get_password("shioaji", "secret_key")  # @secret key
print(api_key)
print(secret_key)
