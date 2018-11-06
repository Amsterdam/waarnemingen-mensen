from rest_framework.test import APITestCase

from .factories import PassageFactory

TEST_POST = {
    "type": "passage-v1",
    "id": "cbbd2efc-78f4-4d41-bf5b-4cbdf1e87269",
    "data": {
        "datumTijd": "2018-10-16T12:13:44Z",
        "straat": "Spaarndammerdijk",
        "rijstrook": 1,
        "rijrichting": 1,
        "cameraId": "ddddffff-4444-aaaa-7777-aaaaeeee1111",
        "cameraNaam": "Spaarndammerdijk [Z]",
        "cameraKijkrichting": 0,
        "cameraLocatie": {
            "type": "Point",
            "coordinates": [
                4.845423,
                52.386831
            ]
        }
    },
    "kentekenLand": "NL",
    "kentekenNummerBetrouwbaarheid": 640,
    "kentekenLandBetrouwbaarheid": 690,
    "kentekenKaraktersBetrouwbaarheid": [
        {
            "betrouwbaarheid": 650,
            "positie": 1
        },
        {
            "betrouwbaarheid": 630,
            "positie": 2
        },
        {
            "betrouwbaarheid": 640,
            "positie": 3
        },
        {
            "betrouwbaarheid": 660,
            "positie": 4
        },
        {
            "betrouwbaarheid": 620,
            "positie": 5
        },
        {
            "betrouwbaarheid": 640,
            "positie": 6
        }
    ],
    "indicatieSnelheid": 23,
    "automatischVerwerkbaar": True,
    "voertuigSoort": "Bromfiets",
    "merk": "SYM",
    "inrichting": "N.V.t.",
    "datumEersteToelating": "2015-03-06",
    "datumTenaamstelling": "2015-03-06",
    "toegestaneMaximumMassaVoertuig": 249,
    "europeseVoertuigcategorie": "L1",
    "europeseVoertuigcategorieToevoeging": "e",
    "taxIndicator": True,
    "maximaleContructiesnelheidBromSnorfiets": 25,
    "brandstoffen": [
        {
            "brandstof": "Benzine",
            "volgnr": 1
        }
    ]
}


class PassageAPITest(APITestCase):
    """
    Test the passage endpoint
    """

    def setUp(self):
        self.URL = '/iotsignals/milieuzone/passage/'
        self.p = PassageFactory()

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
        # in the setup we also create a csv.
        self.assertEqual(res.data['count'], 2)

    def test_get_passage(self):
        """ Test getting a passage """
        res = self.client.get('{}{}/'.format(self.URL, self.p.id))

        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.data['id'], self.p.id)

    def test_update_passages_not_allowed(self):
        """ Test if updating a passage is not allowed """
        res = self.client.put('{}{}/'.format(
            self.URL, self.p.id), dict(merk='DummyMerk'))

        self.assertEqual(res.status_code, 405)

        res = self.client.put('{}{}/'.format(
            self.URL, self.p.id), TEST_POST, format='json')

        self.assertEqual(res.status_code, 405)

    def test_delete_passages_not_allowed(self):
        """ Test if deleting a passage is not allowed """
        res = self.client.delete('{}{}/'.format(self.URL, self.p.id))

        self.assertEqual(res.status_code, 405)

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

        # Make a request with a voertuig_soort filter and check if the result is
        # correct
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
        passage_nl = PassageFactory(kenteken_land='NL')
        passage_nl.save()
        passage_es = PassageFactory(kenteken_land='ES')
        passage_es.save()

        # Make a request with a kenteken_land filter and check if the result is
        # correct
        url = '{}{}'.format(self.URL, '?kenteken_land=NL')
        res = self.client.get(url)
        self.assertEqual(res.data['count'], 1)
        self.assertEqual(res.data['results'][0]['id'], passage_nl.id)
