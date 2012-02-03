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

    If SUBCLASS-OF(N, X)
        // see [#subc]_ footnote
        subclasses = list()
        superclasses = list()
        For XC in children of X
            If EQUIVALENT(N, XC)
                COMPARE-CLASSES(N, XC)
                STOP
            If SUBCLASS-OF(N, XC)
                APPEND(superclasses, XC)
            If SUPERCLASS-OF(N, XC)
                APPEND(subclasses, XC)
        If superclasses is empty
            LINK(N IS-A X)
        Else
            LINK(N IS-A superclasses)

        Disconnect all subclasses
        LINK(subclasses IS-A N)

    If SUPERCLASS-OF(N, X)
        For XP in parents of X
            


.. [#subc] since all children are siblings, if N is a subclass of any XC, then
    it can be a subclass of more than one children together (multiple inheritance)
    similarly if N is a superclass of any XC, it can be a superclass of multiple
    XC, so we need to categorize
    If N is equivalent to any children, since the children themselves are not
    equivalent to each other, we can immediately stop inferring any other
    relation
