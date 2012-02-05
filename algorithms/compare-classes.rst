`COMPARE-CLASSES(N, X)`
=======================

N
    Node representing class to be inserted.

X
    Node representing class being compared against. X is part of the Cluster
    Space Graph and can be navigated via relationships.

::

    If EQUIVALENT(N, X)
    Then
        Add Equivalent Edge between N and X
        END

    If DIRECT-SUBCLASS-OF(N, X)
        // see [#subc]_ footnote
        For XC in children of X
            If EQUIVALENT(N, XC)
                COMPARE-CLASSES(N, XC)
                STOP
            If DIRECT-SUPERCLASS-OF(N, XC)   # Intermediary
                DISCONNECT(X, XC)
                LINK(XC IS-A N)

        LINK(N IS-A X)                   # Child

        For XS in SIBLINGS(X) (spanning all parents)  # Multiple Inheritance
            If DIRECT-SUBCLASS-OF(N, XS)
                COMPARE-CLASSES(N, XS)

    Else If DIRECT-SUPERCLASS-OF(N, X)
        LINK(X IS-A N)                       # Parent
        For XO in (SIBLINGS(X) OR ROOTS)
            If DIRECT-SUPERCLASS-OF(N, XO)
                LINK(XO IS-A N)

        Set ROOTS = ROOTS - [X] + [N]        # New root


.. [#subc] since all children are siblings, if N is a subclass of any XC, then
    it can be a subclass of more than one children together (multiple inheritance)
    similarly if N is a superclass of any XC, it can be a superclass of multiple
    XC, so we need to categorize
    If N is equivalent to any children, since the children themselves are not
    equivalent to each other, we can immediately stop inferring any other
    relation

