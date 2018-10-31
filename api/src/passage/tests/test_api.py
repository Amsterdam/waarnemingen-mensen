import datetime
import json
from rest_framework.test import APITestCase

from passage.tests.factories import PassageFactory

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


class PassagePostAPITest(APITestCase):
    """
    Test POSTing to /milieu/passage/
    """

    def setUp(self):
        self.URL = '/iotsignals/milieuzone/passage/'

        # self.p = PassageFactory().create()
        # self.w = factories.WellFactory()
        # self.c = factories.ContainerFactory(
        #     container_type=self.t,
        #     well=self.w
        # )
        # self.s = factories.SiteFactory()
        # self.w.site_id = self.s.id
        # self.w.save()
        # self.snull = factories.SiteFactory()
        # self.snull.short_id = None
        # self.snull.save()
        #
        # self.k = kilofactory.KiloFactory()
        # self.k.site_id = self.s.id
        # self.k.container_id = self.c.id
        # self.k.save()
        #
        # kilofactory.make_stats_values(self.s)

    def test_post_new_passage(self):
        res = self.client.post(self.URL, TEST_POST, format='json')

        self.assertEqual(res.status_code, 201, res.data)

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

    def test_index_pages(self):
        url = "iotsignals"

        response = self.client.get("/{}/".format(url))

        self.assertEqual(
            response.status_code, 200, "Wrong response code for {}".format(url)
        )

    # def test_lists(self):
    #     for url in self.datasets:
    #         response = self.client.get("/{}/".format(url))
    #
    #         self.assertEqual(
    #             response.status_code, 200,
    #             "Wrong response code for {}".format(url)
    #         )
    #
    #         # default should be json
    #         self.valid_response(url, response, 'application/json')
    #
    #         self.assertEqual(
    #             response["Content-Type"],
    #             "application/json",
    #             "Wrong Content-Type for {}".format(url),
    #         )
    #
    #         self.assertIn(
    #             "count", response.data, "No count attribute in {}".format(url)
    #         )
    #
    # def test_lists_html(self):
    #     for url in self.datasets:
    #         response = self.client.get("/{}/?format=api".format(url))
    #
    #         self.valid_response(url, response, 'text/html; charset=utf-8')
    #
    #         self.assertIn(
    #             "count", response.data, "No count attribute in {}".format(url)
    #         )
    #
    # def test_lists_csv(self):
    #     for url in self.datasets:
    #         response = self.client.get("/{}/?format=csv".format(url))
    #
    #         self.valid_response(url, response, 'text/csv; charset=utf-8')
    #
    #         self.assertIn(
    #             "count", response.data, "No count attribute in {}".format(url)
    #         )
    #
    # def test_lists_xml(self):
    #     for url in self.datasets:
    #         response = self.client.get("/{}/?format=xml".format(url))
    #
    #         self.valid_response(
    #             url, response, 'application/xml; charset=utf-8')
    #
    #         self.assertIn(
    #             "count", response.data, "No count attribute in {}".format(url)
    #         )
    #
    # def test_site_filters(self):
    #     url = "afval/sites"
    #     response = self.client.get(f"/{url}/", {'short_id': self.s.short_id})
    #     self.valid_response(url, response, 'application/json')
    #     self.assertEqual(response.data['count'], 1)
    #     self.assertEqual(int(response.data['results'][0]['id']), self.s.id)
    #
    # def test_site_null_filter(self):
    #     url = "afval/sites"
    #     response = self.client.get(f"/{url}/", {'no_short_id': 1})
    #     self.valid_response(url, response, 'application/json')
    #     self.assertEqual(response.data['count'], 1)
    #     self.assertEqual(int(response.data['results'][0]['id']), self.snull.id)
