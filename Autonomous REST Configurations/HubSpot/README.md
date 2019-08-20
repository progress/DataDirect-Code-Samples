## HubSpot Configuration

#### Get Access Token for HubSpot

1. If you don't have a HubSpot Developer account, sign up for a [trial](https://app.hubspot.com/login/?loginRedirectUrl=https%3A%2F%2Fapp.hubspot.com%2Fdeveloper%2F).    
2. Go to HubSpot Developer Console and create new App, by clicking on `Create App` button.  
3. Configure your new app as shown below. You can choose your own `OAuth Redirect URL` or just use the below configuration.  
![configure new client](https://raw.githubusercontent.com/progress/DataDirect-Code-Samples/master/Autonomous%20REST%20Configurations/HubSpot/img/Capture.PNG)  
4. Once you have created the new app, you should be able to see ClientID and ClientSecret for your account. Save them.  
5. HubSpot uses OAuth2.0 authentication, so to get access token you need to get the Authorization code first. To get the Authorization code, you need to make a request to Authorization Server by sending your `ClientId`, `Authorization Scopes` (i.e, to indicate the data you want to access) and `OAuth Redirect URL` which you specified when you created the client in step 3.  
6. To get the authorization code, modify the below URL with your `Client ID` and `OAuth Redirect URL` if you have used something else.

          https://app.hubspot.com/oauth/authorize?scope=contacts%20content%20reports%20automation%20oauth%20forms%20tickets&redirect_uri=https://www.progress.com&client_id=your-client-id  
7. <strong>Note: </strong> The above authorization request only includes the below scopes.You can access the [full list of scopes](https://developers.hubspot.com/docs/methods/oauth2/initiate-oauth-integration#scopes) here to get access to more data from Hubspot
          
          Contacts, Content, Reports, Automation, OAuth, Forms, Tickets
          
8. Copy the above URL and paste in your browser. You should now see a screen like below, after you have logged in with your HubSpot account. Click on `Accept`.   
9. After you click on `Accept` you should be redirected to www.progress.com or the redirect URL you specified. The URL should be in the below format and you will find your Authorization Code in the code parameter in the URL as shown below.  

          https://www.progress.com/?code=your-Authorization-Code
![Authorization Code](https://github.com/progress/DataDirect-Code-Samples/blob/master/Autonomous%20REST%20Configurations/HubSpot/img/Capture2.PNG?raw=true) 

10. Copy the Authorization Code and send a new `POST` request to HubSpot Authorization server's endpoint `https://api.hubapi.com/oauth/v1/token` to get the access token, which is the final piece of the puzzle. 

11. To get the `Access Token`, edit the below url to include your `Client ID`, `Client Secret`, `Redirect URL` from step 3 and the `Authorization Code` we got from the step 8.

          https://api.hubapi.com/oauth/v1/token?client_id=your-client-id&client_secret=your-client-secret&redirect_uri=https://www.progress.com&grant_type=authorization_code&code=your-authorization-code

12. Use CURL or Postman to send a POST request using the above URL and you should get your access token in the below format. 

```javascript
{
    "refresh_token": "your-refresh-token",
    "access_token": "your-access-token",
    "expires_in": 21600
}
```

13. With `Access Token` and `Refresh Token` in your hand, you are now ready to make requests to access data from your HubSpot instance.

#### Configure Connection

14. If you are using Autonomous REST JDBC connector, you can use the below JDBC URL to connect to HubSpot -  

```java
jdbc:datadirect:autorest:config="/path-to/hubspot.rest";authenticationmethod=OAuth2;clientid=your-client-id;clientsecret=your-client-secret;refreshtoken=your-refresh-token;tokenuri=https://api.hubapi.com/oauth/v1/token
```

15. If you are using Autonomous REST ODBC connector, you can use the below ODBC configuration to connect to HubSpot.  

![Configure Zoho ODBC - Part 1](https://raw.githubusercontent.com/progress/DataDirect-Code-Samples/master/Autonomous%20REST%20Configurations/HubSpot/img/Capture4.PNG)  

![Configure Zoho ODBC - Part 2](https://raw.githubusercontent.com/progress/DataDirect-Code-Samples/master/Autonomous%20REST%20Configurations/HubSpot/img/Capture3.PNG)  

#### API Documentation
16. You can find the documenation for HubSpot [here](https://developers.hubspot.com/docs/overview)

#### Notes
17. The configurations `hubspot.rest` and `hubspot_beforesampling.rest` do not cover all the endpoints in the HubSpot API [documentation](https://developers.hubspot.com/docs/overview). It only covers a part of the Hubspot API as I didn't have an instance filled with data. If you need data from any other endpoint, feel free to add it to the `.rest` file.  



