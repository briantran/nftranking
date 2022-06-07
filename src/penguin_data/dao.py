import math

from src.const import PENGUIN_COLLECTION_SIZE


class Penguin:
    FEATURES = ('background', 'skin', 'body', 'face', 'head')

    def __init__(self, token, background, skin, body, face, head):
        self.token = token
        self.background = background
        self.skin = skin
        self.body = body
        self.face = face
        self.head = head

    def insert_values(self):
        return [self.token] + [getattr(self, feature) for feature in self.FEATURES]

    def rarity_score(self, feature_count_dict):
        return sum(1 / feature_rarity for feature_rarity in self._feature_rarities(feature_count_dict).values())

    def statistical_score(self, feature_count_dict):
        return math.prod(self._feature_rarities(feature_count_dict).values())

    def _feature_rarity(self, feature, feature_count_dict):
        number_penguins_sharing_trait = feature_count_dict[feature][getattr(self, feature)]
        return number_penguins_sharing_trait / PENGUIN_COLLECTION_SIZE

    def _feature_rarities(self, feature_count_dict):
        return dict((feature, self._feature_rarity(feature, feature_count_dict)) for feature in Penguin.FEATURES)

    @staticmethod
    def from_json(token, json_struct):
        attributes = json_struct['attributes']
        args = {'token': token}

        feature_dict = dict((attribute['trait_type'].lower(), attribute['value']) for attribute in attributes)
        if set(feature_dict.keys()) != set(Penguin.FEATURES):
            raise Exception(f'{token} has invalid features')

        args.update(feature_dict)
        return Penguin(**args)
