
register 'datafu-0.0.10.jar';
register 'trove4j-3.0.3.jar';
register 'recsys-udfs.jar';

define recsys__Enumerate 
    datafu.pig.bags.Enumerate('1');

register 'recsys.py' using jython as recsys_udfs;

----------------------------------------------------------------------------------------------------
/*
 * This file contains the basic macros used in macros/recommenders.pig for the various steps of 
 * building recommendations.
 *
 */
----------------------------------------------------------------------------------------------------

/*
 * This is the first step recommendation engine
 *
 * Build a weighted graph of item-item links from a collection of user-item signals.
 *
 * Algorithmically, this is "contracting" the bipartite user-item graph into a regular
 * graph of item similarities. If a user U has affinity with items I1 and I2 of weights
 * W1 and W2 respectively, than a link * between I1 and I2 is formed with the weight
 * MIN(W1, W2).
 *
 * Input:
 *      ui_signals: { (user:chararray, item:chararray, weight:float} )
 *      logistic_param: float       Influences how multiple links between a user and item are
 *                                  combined.  See params/README.md for details.
 *      min_link_weight: float      For performance any item-item links lower than this value
 *                                  will be removed.  See params/README.md for details.
 *      max_links_per_user: int     For performance only keep the top [max_links_per_user link]
 *                                  for an individual user.  See params/README.md for details.
 *
 * Output:
 *      ii_links: { (item_A:chararray, item_B:chararray, weight:float) }
 *      item_weights: { (item:int, overall_weight:float) }
 *                                 item_weights contains an overall popularity weight for each item.
 */
define recsys__BuildItemItemGraph(ui_signals, logistic_param, min_link_weight, max_links_per_user)
returns ii_links, item_weights {

    define recsys__UserItemToItemItemGraphBuilder
        com.mortardata.recsys.UserItemToItemItemGraphBuilder();
    define recsys__FilterItemItemLinks
        com.mortardata.recsys.FilterItemItemLinks('$min_link_weight');

    ui_signals      =   filter $ui_signals by user is not null and item is not null;

    -- Aggregate events by (user,item) and sum weights to get one weight for each user-item combination.
    ui_agg          =   foreach (group ui_signals by (user, item)) generate
                            flatten(group) as (user, item),
                            (float) SUM($1.weight) as weight;

    -- Apply logistic function to user-item weights so a user with tons of events for the same item
    -- faces diminishing returns.
    ui_scaled       =   foreach ui_agg generate
                            user, item,
                            (float) recsys_udfs.logistic_scale(weight, $logistic_param)
                            as weight;

    -- Sum up the scaled weights for each item to determine its overall popularity weight.
    item_weights_tmp =   foreach (group ui_scaled by item) generate
                            group as item, (float) SUM($1.weight) as overall_weight, $1 as ui;
    $item_weights    =   foreach item_weights_tmp generate item, overall_weight;

    -- Drop items that don't meet the minimum weight.
    ui_filt         =   foreach (filter item_weights_tmp by overall_weight >= $min_link_weight) generate
                            flatten(ui) as (user, item, weight);

    -- Turn the user-item links into an item-item graph where each link is above the
    -- minimum required weight.
    ii_link_terms  =   foreach (group ui_filt by user) {
                            top_for_user = TOP($max_links_per_user, 2, $1);
                            generate flatten(recsys__UserItemToItemItemGraphBuilder(top_for_user));
                        }
    $ii_links      =   foreach (group ii_link_terms by item_A) generate
                            group as item_A,
                            flatten(recsys__FilterItemItemLinks($1))
                                  as (item_B, weight);
};