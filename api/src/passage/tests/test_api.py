from rest_framework.test import APITestCase

from .factories import PassageFactory

TEST_POST = {
    "id": "c56a4180-65aa-42ec-a945-5fd21dec0538",
    "_display": "Passage object (c56a4180-65aa-42ec-a945-5fd21dec0538)",
    "versie": "k",
    "data": {},
    "kenteken_land": "nl",
    "kenteken_nummer_betrouwbaarheid": 101,
    "kenteken_land_betrouwbaarheid": 1,
    "kenteken_karakters_betrouwbaarheid": [],
    "indicatie_snelheid": 200.0,
    "automatisch_verwerkbaar": False,
    "voertuig_soort": "A",
    "merk": "B",
    "inrichting": "C",
    "datum_eerste_toelating": "2018-10-19",
    "datum_tenaamstelling": "2018-10-27",
    "toegestane_maximum_massa_voertuig": 8000,
    "europese_voertuig_categorie": "xv",
    "europese_voertuig_categorie_toevoeging": "l",
    "tax_indicator": False,
    "maximale_constructie_snelheid_bromsnorfiets": 33,
    "brandstoffen": []
}


class PassageAPITest(APITestCase):
    """
    Test the passage endpoint
    """

    def setUp(self):
        self.URL = '/iotsignals/milieuzone/passage/'

    def valid_response(self, url, response, content_type):
        """Check common status/json."""
        self.assertEqual(
            200, response.status_code, "Wrong response code for {}".format(url)
        )

        self.assertEqual(
            f"{content_type}",
            response["Content-Type"],
            "Wrong Content-Type for {}".format(url),
        )

    def test_post_new_passage(self):
        """ Test posting a new passage """
        res = self.client.post(self.URL, TEST_POST, format='json')

        self.assertEqual(res.status_code, 201, res.data)
        for k, v in TEST_POST.items():
            self.assertEqual(res.data[k], v)

    def test_list_passages(self):
        """ Test listing all passages """
        PassageFactory.create()
        res = self.client.get(self.URL)

        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.data['count'], 1)

    def test_get_passage(self):
        """ Test getting a passage """
        passage = PassageFactory.create()
        res = self.client.get('{}{}/'.format(self.URL, passage.id))

        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.data['id'], passage.id)

    def test_default_response_is_json(self):
        """ Test if the default response of the API is on JSON """
        res = self.client.get(self.URL)
        self.valid_response(self.URL, res, 'application/json')

    def test_xml_response(self):
        """ Test XML response """
        url = '{}{}'.format(self.URL, '?format=xml')
        res = self.client.get(url)
        self.valid_response(url, res, 'application/xml; charset=utf-8')

    def test_csv_response(self):
        """ Test CSV response """
        url = '{}{}'.format(self.URL, '?format=csv')
        res = self.client.get(url)
        self.valid_response(url, res, 'text/csv; charset=utf-8')

    def test_html_response(self):
        """ Test HTML response """
        url = '{}{}'.format(self.URL, '?format=api')
        res = self.client.get(url)
        self.valid_response(url, res, 'text/html; charset=utf-8')

    def test_merk_filters(self):
        """ Test filtering on 'merk'"""
        # Create two passages with a different 'merk' value
        passage_ferrari = PassageFactory(merk='ferrari')
        passage_ferrari.save()
        passage_fiat = PassageFactory(merk='fiat')
        passage_fiat.save()

        # Make a request with a merk filter and check if the result is correct
        url = '{}{}'.format(self.URL, '?merk=ferrari')
        res = self.client.get(url)
        self.assertEqual(res.data['count'], 1)
        self.assertEqual(res.data['results'][0]['id'], passage_ferrari.id)

    def test_voertuig_soort_filters(self):
        """ Test filtering on 'voertuig_soort'"""
        # Create two passages with a different 'voertuig_soort' value
        passage_bus = PassageFactory(voertuig_soort='bus')
        passage_bus.save()
        passage_fiets = PassageFactory(voertuig_soort='fiets')
        passage_fiets.save()

        # Make a request with a voertuig_soort filter and check if the result is correct
        url = '{}{}'.format(self.URL, '?voertuig_soort=bus')
        res = self.client.get(url)
        self.assertEqual(res.data['count'], 1)
        self.assertEqual(res.data['results'][0]['id'], passage_bus.id)

    def test_versie_filters(self):
        """ Test filtering on 'versie'"""
        # Create two passages with a different 'versie' value
        passage_v1 = PassageFactory(versie='1')
        passage_v1.save()
        passage_v2 = PassageFactory(versie='2')
        passage_v2.save()

        # Make a request with a versie filter and check if the result is correct
        url = '{}{}'.format(self.URL, '?versie=1')
        res = self.client.get(url)
        self.assertEqual(res.data['count'], 1)
        self.assertEqual(res.data['results'][0]['id'], passage_v1.id)

    def test_kenteken_land_filters(self):
        """ Test filtering on 'kenteken_land'"""
        # Create two passages with a different 'kenteken_land' value
        passage_NL = PassageFactory(kenteken_land='NL')
        passage_NL.save()
        passage_ES = PassageFactory(kenteken_land='ES')
        passage_ES.save()

        # Make a request with a kenteken_land filter and check if the result is correct
        url = '{}{}'.format(self.URL, '?kenteken_land=NL')
        res = self.client.get(url)
        self.assertEqual(res.data['count'], 1)
        self.assertEqual(res.data['results'][0]['id'], passage_NL.id)
