%default INPUT_PATH_PURCHASES './data/purchases.json'
%default INPUT_PATH_WISHLIST './data/wishlists.json'
%default OUTPUT_PATH './output/recommendations.json'


/******* Load some synthetic data**********/

--Get purchase signals
purchase_input = load '$INPUT_PATH_PURCHASES' using org.apache.pig.piggybank.storage.JsonLoader(
                    'row_id: int,
                     movie_id: chararray,
                     movie_name: chararray,
                     user_id: chararray,
                     purchase_price: int');

--Get wishlist signals
wishlist_input =  load '$INPUT_PATH_WISHLIST' using org.apache.pig.piggybank.storage.JsonLoader(
                     'row_id: int,
                      movie_id: chararray,
                      movie_name: chararray,
                      user_id: chararray');



/******* Convert Data to Signals **********/

-- Start with choosing 1 as max weight for a signal.
purchase_signals = foreach purchase_input generate
                        user_id    as user,
                        movie_name as item,
                        1.0        as weight;


-- Start with choosing 0.5 as weight for wishlist items because that is a weaker signal than purchasing an item.
wishlist_signals = foreach wishlist_input generate
                        user_id    as user,
                        movie_name as item,
                        0.5        as weight;  

user_signals = union purchase_signals, wishlist_signals;



rmf $OUTPUT_PATH/user_signals;

store user_signals into '$OUTPUT_PATH/user_signals' using PigStorage();
