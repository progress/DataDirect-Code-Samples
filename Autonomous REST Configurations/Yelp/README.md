## Yelp Configuration

#### Get your API Keys
1. Go to [Yelp Developers](https://www.yelp.com/developers/documentation/v3) page, create a new application.  
2. After you have created the application, you should find the API key for Yelp.

#### Using API Key
3. Yelp uses Header based Authentication. Configure Autonomous REST Connector using the below connection properties for a successful connection.
          
          authenticationMethod: HttpHeader
          authheader: Authorization
          SecurityToken: Bearer <Yelp API Key>

#### API Documentation
4. You can find the Yelp Documentation [here](https://www.yelp.com/developers/documentation/v3/get_started)

#### API Notes:
5. Reviews are limited to just 3 records  
6. Pagination is only available on Business Search endpoint.  
7. Maximum value for offset+limit is 1000. So you will only get maximum of 1000 records from Business Search.  
8. Transaction Search returns only 20 records. Doesn't offer pagination.


#### Covered Endpoints

Business Search    : https://www.yelp.com/developers/documentation/v3/business_search  
Phone Search       : https://www.yelp.com/developers/documentation/v3/business_search_phone  
Transaction Search : https://www.yelp.com/developers/documentation/v3/transaction_search  
Business Details   : https://www.yelp.com/developers/documentation/v3/business  
Business Match     : https://www.yelp.com/developers/documentation/v3/business_match  
Reviews            : https://www.yelp.com/developers/documentation/v3/business_reviews  
Event Search       : https://www.yelp.com/developers/documentation/v3/event_search  
Event Lookup       : https://www.yelp.com/developers/documentation/v3/event  
All Category       : https://www.yelp.com/developers/documentation/v3/category
