package com.turbulence.util;

import java.util.Iterator;
import java.util.List;

import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.SuperSlice;

import me.prettyprint.hector.api.exceptions.HectorException;

import me.prettyprint.hector.api.query.SuperSliceQuery;

public class AllSuperColumnsIterator<SN, N, V> implements Iterator<HSuperColumn<SN, N, V>> {
    private SN start;
    private int count;
    Iterator<HSuperColumn<SN, N, V>> columnsIterator;
    SuperSliceQuery<?, SN, N, V> query;
    private boolean isLastIteration;

    public AllSuperColumnsIterator(SuperSliceQuery<?, SN, N, V> query) {
        start = null;
        count = 100;
        columnsIterator = null;
        this.query = query;
        isLastIteration = false;
    }

    public boolean hasNext() {
        if (columnsIterator == null || !columnsIterator.hasNext()) {
            if (isLastIteration)
                return false;

            if (!fetchMore())
                return false;
        }
        return true;
    }

    public HSuperColumn<SN, N, V> next() {
        return columnsIterator.next();
    }

    private boolean fetchMore() {
        try {
            query.setRange(start, null, false, count);
            SuperSlice<SN, N, V> slice = query.execute().get();
            List<HSuperColumn<SN, N, V>> columns = slice.getSuperColumns();
            int origSize = columns.size();

            if (origSize == 0) {
                return false;
            }

            if (origSize >= count)
                start = columns.remove(columns.size()-1).getName();

            columnsIterator = columns.iterator();

            if (origSize < count)
                isLastIteration = true;

            return true;
        } catch (HectorException e) {
            return false;
        }
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }
}
