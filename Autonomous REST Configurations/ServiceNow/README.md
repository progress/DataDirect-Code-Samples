## Service Now Configuration Notes

a. If you don't have a Service Now account, you can register for a developer account [here](https://developer.servicenow.com/app.do#!/home) and request a developer instance.

b. You can find the REST API Documentation [here](https://developer.servicenow.com/app.do#!/rest_api_doc?v=madrid&id=c_TableAPI)

c. This configuration only covers limited number of ServiceNow tables out of thousands. I couldn't cover them as my instance didn't have any data in lot of them. You can add the tables to servicenow_beforesampling.rest for sampling, if you happen to have more tables.

d. Paging is set to 10000 by default by the API, but I have set the pagesize to 1000 to demonstrate paging capabilities of Autonomous REST Connector