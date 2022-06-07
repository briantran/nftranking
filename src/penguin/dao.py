import math

from src.const import PENGUIN_COLLECTION_SIZE


class Penguin:
    """Data container for Penguin NFT metadata.
    """
    FEATURES = ('background', 'skin', 'body', 'face', 'head')

    def __init__(self, token, background, skin, body, face, head):
        self.token = token
        self.background = background
        self.skin = skin
        self.body = body
        self.face = face
        self.head = head

    def calculate_insert_values(self):
        """Calculates values to insert the metadata for this Penguin into PENGUIN_TABLE_NAME.
        """
        return [self.token] + [getattr(self, feature) for feature in self.FEATURES]

    def calculate_rarity_score(self, feature_count_dict):
        """Calculates the rarity of a penguin as the sum of 1/feature rarity for all its features.
        """
        return sum(1 / feature_rarity for feature_rarity in self._calculate_feature_rarities(feature_count_dict).values())

    def calculate_statistical_score(self, feature_count_dict):
        """Calculates the statistical probability that a penguin would have all these features as the product of the
        statistical probability of each of its features.
        """
        return math.prod(self._calculate_feature_rarities(feature_count_dict).values())

    def _calculate_feature_rarity(self, feature, feature_count_dict):
        """Calculates and returns the rarity for `feature` of the `self` Penguin.
        """
        number_penguins_sharing_trait = feature_count_dict[feature][getattr(self, feature)]
        return number_penguins_sharing_trait / PENGUIN_COLLECTION_SIZE

    def _calculate_feature_rarities(self, feature_count_dict):
        """Calculates and returns the rarity for each of the features of the `self` Penguin.
        """
        return dict(
            (feature, self._calculate_feature_rarity(feature, feature_count_dict)) for feature in Penguin.FEATURES
        )

    @staticmethod
    def from_json(token, json_struct):
        """Factory method to construct a `Penguin` from a JSON.
        """
        attributes = json_struct['attributes']
        args = {'token': token}

        feature_dict = dict((attribute['trait_type'].lower(), attribute['value']) for attribute in attributes)
        if set(feature_dict.keys()) != set(Penguin.FEATURES):
            raise Exception(f'{token} has invalid features')

        args.update(feature_dict)
        return Penguin(**args)
