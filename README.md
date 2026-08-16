# webscraping
It shows the scraping archetecture and documentation

## URL BUILDER
This module contains the class that generates valid urls based on the parameters, such as, airport origin, destity, outbound date, etc.


Therefore, the flow of the scraping is: First we receive the solicitation payload then we generate the url, request to this url, then we get the data from the site.