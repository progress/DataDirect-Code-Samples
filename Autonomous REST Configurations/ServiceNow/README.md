## Service Now Configuration

### Get your Developer Instance
1. If you don't have a Service Now account, you can register for a developer account [here](https://developer.servicenow.com/app.do#!/home).

2. Go to `Manage` Tab and Request a new Instance. You should be provided with credentials. 

### Using the Credentials
3. Service Now supports Basic Authentication. Configure Autonomous REST Connector using the below connection properties for a successful connection.
      
        Authentication: Basic
        Username: <Your ServiceNow UserName>
        Password: <Your ServiceNow Password>

### API Documentation
4. You can find the ServiceNow API Documentation [here](https://developer.servicenow.com/app.do#!/rest_api_doc?v=madrid&id=c_TableAPI)

### API Notes
5. This configuration only covers limited number of ServiceNow tables out of thousands. I couldn't cover them as my instance didn't have any data in lot of them. You can add the tables to servicenow_beforesampling.rest for sampling, if you happen to have more tables.

6. Paging is set to 10000 by default by the API, but I have set the pagesize to 1000 to demonstrate paging capabilities of Autonomous REST Connector
