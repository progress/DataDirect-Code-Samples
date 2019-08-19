## ZohoCRM Configuration

#### Get Access Token for ZohoCRM

1. If you don't have a ZohoCRM account, sign up for a [trial](https://www.zoho.com/crm/signup.html).    
2. Go to [Zoho Developer Console](https://accounts.zoho.com/developerconsole) and create new Client, by clicking on `Add Client ID` button.  
3. Configure your new client as shown below. You can choose your own `Client Domain` and `Authorized redirect URIs` or just use the below configuration.  
![configure new client](https://raw.githubusercontent.com/progress/DataDirect-Code-Samples/master/Autonomous%20REST%20Configurations/ZohoCRM/img/Capture.PNG)  
4. Once you have created the new client, you should be able to see ClientID and ClientSecret for your account. Save them.  
5. ZohoCRM uses OAuth2.0 authentication, so to get access token you need to get the Authorization code first. To get the Authorization code, you need to make a request to Authorization Server by sending your `ClientId`, `Authorization Scopes` (i.e, to indicate the data you want to access) and `redirect URL` which you specified when you created the client in step 3.  
6. To get the authorization code, modify the below URL with your `Client ID` and `redirect URL` if you have used something else.

          https://accounts.zoho.com/oauth/v2/auth?scope=ZohoCRM.users.ALL,ZohoCRM.org.all,ZohoCRM.modules.all,ZohoCRM.settings.all&client_id=`your-client-id`&response_type=code&redirect_uri=https://www.progress.com/&access_type=offline  

7. Copy the above URL and paste in your browser. You should now see a screen like below, after you have logged in with your ZohoCRM account. Click on `Accept`.  
![Accept Scope](https://github.com/progress/DataDirect-Code-Samples/blob/master/Autonomous%20REST%20Configurations/ZohoCRM/img/Capture3.PNG?raw=true)  
8. After you click on `Accept` you should be redirected to www.progress.com or the redirect URL you specified. The URL should be in the below format and you will find your Authorization Code in the code parameter in the URL as shown below.  

          https://www.progress.com/?code=authorization_code&location=us&accounts-server=https%3A%2F%2Faccounts.zoho.com
![Authorization Code](https://github.com/progress/DataDirect-Code-Samples/blob/master/Autonomous%20REST%20Configurations/ZohoCRM/img/Capture2.PNG?raw=true) 

9. Copy the Authorization Code and send a new `POST` request to Zoho Authorization server's endpoint `https://accounts.zoho.com/oauth/v2/token` to get the access token, which is the final piece of the puzzle. 

10. To get the `Access Token`, edit the below url to include your `Client ID`, `Client Secret`, `Redirect URL` from step 3 and the `Authorization Code` we got from the step 8.

          https://accounts.zoho.com/oauth/v2/token?grant_type=authorization_code&client_id=your-client-id&client_secret=your-client-secret&redirect_uri=https://www.progress.com/&code=your-authorization-code&prompt=consent

11. Use CURL or Postman to send a POST request using the above URL and you should get your access token in the below format. 

                    {
                        "access_token": "your_access_token",
                        "refresh_token": "your_refresh_token",
                        "expires_in_sec": 3600,
                        "api_domain": "https://www.zohoapis.com",
                        "token_type": "Bearer",
                        "expires_in": 3600000
                    }

12. With `Access Token` and `Refresh Token` in your hand, you are now ready to make requests to access data from your ZohoCRM instance.

#### Configure Connection

1. If you are using Autonomous REST JDBC connector, you can use the below JDBC URL to connect to ZohoCRM -  

          jdbc:datadirect:autorest:config="/path-to/zohocrm.rest";authenticationmethod=OAuth2;clientid=your-client-id;clientsecret=your-client-secret;refreshtoken=your-refresh-token;tokenuri=https://accounts.zoho.com/oauth/v2/token

2. If you are using Autonomous REST ODBC connector, you can use the below ODBC configuration to connect to ZohoCRM.  

![Configure Zoho ODBC - Part 1](https://raw.githubusercontent.com/progress/DataDirect-Code-Samples/master/Autonomous%20REST%20Configurations/ZohoCRM/img/Capture4.PNG)  

![Configure Zoho ODBC - Part 2](https://raw.githubusercontent.com/progress/DataDirect-Code-Samples/master/Autonomous%20REST%20Configurations/ZohoCRM/img/Capture5.PNG)  


          
