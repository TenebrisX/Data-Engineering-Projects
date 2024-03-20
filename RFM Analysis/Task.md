## **Project description**

### Create a data mart for RFM customer segmentation:

    Data mart name: dm_rfm_segments
    Data mart location: de.analysis
    Data location: de.production

    Data mart structure:
        user_id
        recency (number from 1 to 5) - Number of days since the last order
        frequency (number from 1 to 5) - Number of orders
        monetary_value (number from 1 to 5) - Total customer spend 
    Data mart depth: 2022/01/01 - 2022/12/31 
    No updates needed A successful order is an order with status "Closed" 
    Segment users into 5 equal RMF segments, if the number of orders is equal, then order them randomly.
