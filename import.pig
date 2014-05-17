/**
 *  This script imports a csv of transactional data, extracts signals based on user purchase
 *  information and then combine those signals before running a script to create item-item
 *  recommendations. The weight of 1.0 is arbitrarily chosen. If additional sources of data
 *  such as page views or wishlists were added, we could adjust weights accordingly
 */
 
 
-- Get transactional data
bets = load 'results.csv' using PigStorage(',');


-- extract cust_id, superclass and weight(1) from 'bets' relation
superclass_signals = foreach bets generate
						 $0 as cust_id,
                         $1 as superclass,
                         1.0 as weight;
                       

--store superclass_signals into 'signals' using PigStorage();
dump superclass_signals;