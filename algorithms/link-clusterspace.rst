`LINK-CLUSTERSPACE(NODE(C))`
============================

::

    Q = queue of clusterspace roots
    while Q is not empty:
        X = next element in Q
        COMPARE-CLASSES(N, X)
        If N is not linked
            Add DIRECT-SUBCLASSES-OF(X) to Q

    If N is still no linked:
        add N as a new Root

