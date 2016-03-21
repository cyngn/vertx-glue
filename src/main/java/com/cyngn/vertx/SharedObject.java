package com.cyngn.vertx;

import io.vertx.core.shareddata.Shareable;

/**
 * Generic class for holding things we know are shareable across verticles.
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 3/15/16
 */
class SharedObject<T> implements Shareable {

    // the value to share safely
    public final T value;

    public SharedObject(T value) {
        this.value = value;
    }
}
