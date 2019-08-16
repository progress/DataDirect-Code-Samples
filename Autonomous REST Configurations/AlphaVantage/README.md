## AlphaVantage Configuration Notes

#### Get Your API Key
1.  You can get Alphavantage API key from [here](https://www.alphavantage.co/support/#api-key) for free.

#### Using API Key
2. AlphaVantage uses URL Parameter based Authentication. Configure Autonomous REST Connector using the below connection properties for a successful connection.
            
            authenticationMethod: UrlParameter
            authheader: apikey
            securityToken: <AlphaVantage API Key>
#### API Documentation
3. You can find the documentation for AlphaVantage [here](https://www.alphavantage.co/documentation/)

#### API Notes
4. Replace apikey `demo` with your own apikey.  
5. If you are using free API key - you are limited to 500 calls/day.
