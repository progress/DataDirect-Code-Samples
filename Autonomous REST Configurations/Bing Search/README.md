## Bing Search Configuration

### Get your API Key
1. Login into your Azure Portal, search for `Bing Search` and Create a resource. Once your resource is created, you should find the API key in `Quick Start` tab under `Resource Management`.

### Notes
2. The Authentication for Bing Search is Header based. Configure Autonomous REST Connector using the below connection properties for a successful connection.

	authHeader:  Ocp-Apim-Subscription-Key 
	SecurityToken: Your Key

3. There is a `free` offering from Azure, if you are looking to just try it. Choose the appropriate pricing model based on your needs when you create the resource.


	
