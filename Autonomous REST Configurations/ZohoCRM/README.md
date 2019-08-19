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
