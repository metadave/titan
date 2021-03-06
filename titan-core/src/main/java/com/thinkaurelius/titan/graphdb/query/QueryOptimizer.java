package com.thinkaurelius.titan.graphdb.query;

import java.util.List;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public interface QueryOptimizer<Q extends Query<Q>> {

    public List<Q> optimize(Q query);

}
