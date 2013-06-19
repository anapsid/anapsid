class ExactStar(object):
    """An implementation for an exact star.
    Definition of an exact star on ?X:
    a) ES(?X) is a pattern {s p ?X} or {?X p o}, s != ?X, p != ?X, o != ?X
    b) ES(?X) is the union of two exact stars ES1(?X) and ES2(?X) that only
    share variable ?X, that is they don't share other variables.
    """

    def __init__(self, triplet, endpoint):
        self.triplets = []
        self.endpoint = endpoint
        self.variables = set()
        self.add(triplet)
        self.star_variable = []
        self.other_variables = set()
        # if not triplet.subject.constant:
        #     self.star_variable.append(triplet.subject)
        #     self.other_variables.add(triplet.subject)
        # if not triplet.theobject.constant:
        #     self.star_variable.append(triplet.theobject)
        #     self.other_variables.add(triplet.theobject)
        if not triplet.subject.constant:
            self.star_variable = triplet.subject
            self.other_variables = set([triplet.theobject]
                                      if not triplet.theobject.constant else [])
        else:
            self.star_variable = triplet.theobject
            self.other_variables = set()

    def __repr__(self):
        return str((self.triplets, self.variables, self.star_variable, self.other_variables))
        # )

    def __contains__(self, other):
        """Check if variable `other` is in our variables"""
        return other in self.variables

    def get_other_variable(self, triplet):
        """Try to form a star. If successful, it will return the other
        variable (not the star variable), else it returns None"""
        if triplet.subject == triplet.theobject and \
           triplet.theobject == triplet.predicate:
            return None

        # try to form with the subject of triplet
        if triplet.subject == self.star_variable and \
           triplet.theobject not in self:
            return triplet.theobject
        # try to form with the object of triplet
        if triplet.theobject == self.star_variable and \
           triplet.subject not in self:
            return triplet.subject
        return None

    def can_join_as_exact_star(self, triplet):
        return self.get_other_variable(triplet) is not None

    def add(self, triplet):
        """Add `triplets` to this star triplets and variables, doesn't check
        if it is valid"""
        self.triplets.append(triplet)
        if not triplet.subject.constant:
            self.variables.add(triplet.subject)
        if not triplet.theobject.constant:
            self.variables.add(triplet.theobject)

    def join_as_exact_star(self, triplet):
        """Join `triplet` to this star. Checks if it is valid, by the
        definition of star"""
        other_variable = self.get_other_variable(triplet)
        if other_variable is None: return False
        self.other_variables.add(other_variable)
        self.add(triplet)
        return True

class ExactStarWithSatellites(ExactStar):
    """An implementation for an exact star with satellites.
    Definition of an exact star on ?X with satellites:
    a) ESS(?X) is the union of an exact star ES(?X) with a pattern
    {s p ?Y} or {?Y p o} such that ?X != ?Y and ?Y is in the other
    variables of the exact star.
    b) ESS(?X) is the union of two stars with sattelites ESS1(?X) and
    ESS2(?X).
    """

    def can_join_as_satellite(self, triplet):
        # if not triplet.subject.constant and not triplet.theobject.constant:
        #     return False

        if triplet.subject != self.star_variable and \
           triplet.subject in self:
            return True
        if triplet.theobject != self.star_variable and \
           triplet.theobject in self:
            return True
        return False

    def join_as_satellite(self, triplet):
        """Join `triplet` as a satellite to this star. Check if it is valid,
        by definition of satellite"""
        if self.can_join_as_satellite(triplet):
            self.add(triplet)
