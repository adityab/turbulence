`LINK-CLUSTERSPACE(NODE(C))`
============================

::

    Q = queue of clusterspace roots
    For X in Q:
        COMPARE-CLASSES(N, X)

    If Q is empty and N is still no linked:
        Set N as a new root                    # Loner

