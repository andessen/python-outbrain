import pytz
import requests
import simplejson as json
import yaml


class OutbrainAmplifyApi(object):

    def __init__(self, outbrain_config=None):
        if not outbrain_config:
            outbrain_config = yaml.load(open('outbrain.yml', 'r'))
        self.user = outbrain_config['user']
        self.password = outbrain_config['password']
        self.base_url = outbrain_config['base_url']
        if not self.base_url.endswith('/'):
            self.base_url += '/'
        self.verify = outbrain_config.get('verify', True)
        if not self.verify:
            requests.packages.urllib3.disable_warnings()
        self.token = self.get_token(self.user, self.password)
        self.locale = pytz.timezone("US/Eastern")  # Outbrain's reporting is in Eastern time

    def _request(self, path, params={}):
        print path, params
        url = self.base_url + path
        r = requests.get(url, headers={'OB-TOKEN-V1': self.token}, params=params, verify=self.verify)
        print r.text
        if r.status_code == 404:
            return None
        return json.loads(r.text)

    def _post_request(self, path, params={}):
        print "POST", path, params
        url = self.base_url + path
        r = requests.post(url, headers={'OB-TOKEN-V1': self.token}, params=params, verify=self.verify)
        print r.text
        if r.status_code == 404:
            return None
        return json.loads(r.text)

    def _put_request(self, path, params={}):
        print "PUT", path, params
        url = self.base_url + path
        r = requests.put(url, headers={'OB-TOKEN-V1': self.token}, params=params, verify=self.verify)
        print r.text
        if r.status_code == 404:
            return None
        return json.loads(r.text)

    def _detete_request(self, path, params={}):
        print "DELETE", path, params
        url = self.base_url + path
        r = requests.delete(url, headers={'OB-TOKEN-V1': self.token}, params=params, verify=self.verify)
        print r.text
        if r.status_code == 404:
            return None
        return json.loads(r.text)

    def get_token(self, user, password):
        token_url = self.base_url + 'login'
        basic_auth = requests.auth.HTTPBasicAuth(user, password)
        r = requests.get(token_url, auth=basic_auth, verify=self.verify)
        results = json.loads(r.text)
        return results['OB-TOKEN-V1']

    # ----------------------------------------------------------------------------------------------
    # Methods to acquire marketer information
    # ----------------------------------------------------------------------------------------------
    def get_marketer(self, marketer_id):
        path = 'marketers/{0}'.format(marketer_id)
        result = self._request(path)
        return result

    def get_marketers(self):
        path = 'marketers'
        results = self._request(path)
        return results.get('marketers', [])

    def get_marketer_ids(self):
        marketers = self.get_marketers()
        return [marketer['id'] for marketer in marketers]

    # ----------------------------------------------------------------------------------------------
    # Methods to acquire budget information
    # ----------------------------------------------------------------------------------------------
    def get_budget(self, budget_id):
        path = 'budgets/{0}'.format(budget_id)
        result = self._request(path)
        return result

    def get_budgets_per_marketer(self, marketer_ids):
        budgets = {}
        for marketing_id in marketer_ids:
            path = '/marketers/{0}/budgets'.format(marketing_id)
            results = self._request(path)
            marketer_budgets = results.get('budgets', [])
            budgets[marketing_id] = marketer_budgets
        return budgets

    # ----------------------------------------------------------------------------------------------
    # Methods to acquire campaign information
    # ----------------------------------------------------------------------------------------------
    def get_campaign(self, campaign_id):
        path = 'campaigns/' + campaign_id
        result = self._request(path)
        return result

    def get_campaign_ids(self, include_archived=True):
        return [c['id'] for c in self.get_campaigns(include_archived=include_archived)]

    def get_campaigns(self, include_archived=True):
        return [c for c in self._yield_all_campaigns(include_archived=include_archived)]

    def _yield_all_campaigns(self, marketer_ids = [], include_archived=True):
        if not marketer_ids:
            marketer_ids = self.get_marketer_ids()
        marketer_campaigns = self.get_campaigns_per_marketer(marketer_ids, include_archived=include_archived)
        for m in marketer_campaigns.keys():
            for c in marketer_campaigns[m]:
                yield c

    def get_campaigns_per_budget(self, budget_ids):
        campaigns = {}
        for budget_id in budget_ids:
            path = 'budgets/{0}/campaigns'.format(budget_id)
            results = self._request(path)
            budget_campaigns = results.get('campaigns', [])
            campaigns[budget_id] = budget_campaigns
        return campaigns

    def get_campaigns_per_marketer(self, marketing_ids, include_archived=True):
        campaigns = {}
        for marketing_id in marketing_ids:
            path = 'marketers/{0}/campaigns'.format(marketing_id)
            params = {'include_archived': 'true' if include_archived else 'false'}
            results = self._request(path, params)
            marketer_campaigns = results.get('campaigns', [])
            campaigns[marketing_id] = marketer_campaigns
        return campaigns

    # ----------------------------------------------------------------------------------------------
    # Methods to create and update entities
    # ----------------------------------------------------------------------------------------------
    def create_budget(self, marketer_id, name, amount, start_date, end_date, runForever, type, pacing, dailyTarget):
        params = """
           {{
            "name": "{}",
            "amount": {},
            "startDate": "{}",
            "endDate": "{}",
            "runForever": "{}"
            "type": "{}"
            "pacing": "{}",
            "dailyTarget": "{}"
            }}
        """.format(name, amount, start_date, end_date, runForever, type, pacing, dailyTarget)
        path = "marketers/{}/budgets".format(marketer_id)
        results = self._post_request(path, params)
        return results

    def change_budget(self, budget_id,  amount=None, start_date=None, end_date=None, runForever = None, dailyTarget = None):
        values = {"id" : budget_id}
        if amount is not None:
            values["amount"] = amount
        if start_date is not None:
            values["start_date"] = start_date
        if end_date is not None:
            values["end_date"] = end_date

        if runForever is not None:
            values["runForever"] = runForever

        if dailyTarget is not None:
            values["dailyTarget"] = dailyTarget

        params = json.dumps(values)
        path = "budgets/{}".format(budget_id)
        results = self._put_request(path, params)
        return results


    def create_campaign(self, name, cpc, budgetId, platforms, tracking_code, feeds):
        params = """
          {{
            "name": "{}",
            "cpc": {},
            "enabled": true,
            "budgetId": "{}",
            "targeting": {{
              "platform": [
                {}
              ]
            }},
            "suffixTrackingCode": "{}",
            "feeds": [
              {}
            ]
          }}
        """.format(name, cpc, budgetId, ", ".join(platforms), tracking_code, ", ".join(feeds))
        path = "campaigns"
        results = self._post_request(path, params)
        return results

    def change_campaign(self, campaign_id, name=None, enabled=None, platforms=None, tracking_code=None):
        values = {"id": campaign_id}
        if name is not None:
            values["name"] = name
        if enabled is not None:
            values["enabled"] = enabled
        if platforms is not None:
            values["targeting"] = {"platform": platforms}
        if tracking_code is not None:
            values["suffixTrackingCode"] = tracking_code
        params = json.dumps(values)

        path = "campaigns/{}".format(campaign_id)
        results = self._put_request(path, params)
        return results

    def create_promoted_link(self, campaign_id, text, url, imageUrl):
        params = """
          {{"text": "{}",
            "url": "{}",
            "enabled": true,
            "imageUrl": "{}"
          }}
        """.format(text, url, imageUrl)
        path = "campaigns/{}/promotedLinks".format(campaign_id)
        results = self._post_request(path, params)
        return results

    def change_promoted_link(self, link_id, enabled):
        values = {"id": link_id}
        values["enabled"] = enabled
        params = json.dumps(values)
        path = "promotedLinks/{}".format(link_id)
        results = self._post_request(path, params)
        return results

    # ----------------------------------------------------------------------------------------------
    # Methods to acquire specific performance information
    # ----------------------------------------------------------------------------------------------
    def get_campaign_performace_per_promoted_link(self, campaign_ids, start_day, end_day):
        """
        :returns: dict[campaign_id][publisher_id] = performance_data
        """
        performance = dict()
        for c in campaign_ids:
            path = 'campaigns/{0}/performanceByPromotedLink'.format(c)
            performance[c] = dict()
            print "_page_performance_data"
            result = self._page_performance_data(path, start_day, end_day)
            for data in result:
                performance[c][data['id']] = data
        return performance

    def get_campaign_performace_per_publisher(self, campaign_ids, start_day, end_day):
        """
        :returns: dict[campaign_id][publisher_id] = performance_data
        """
        performance = dict()
        for c in campaign_ids:
            path = 'campaigns/{0}/performanceByPublisher'.format(c)
            performance[c] = dict()
            result = self._page_performance_data(path, start_day, end_day)
            for data in result:
                performance[c][data['id']] = data
        return performance

    def get_marketers_performace_per_section(self, marketer_ids, start_day, end_day):
        """
        :returns: dict[marketer_id][section] = performance_data
        """
        performance = dict()
        for m in marketer_ids:
            path = 'marketers/{0}/performanceBySection'.format(m)
            performance[m] = dict()
            result = self._page_performance_data(path, start_day, end_day)
            for data in result:
                performance[m][data['id']] = data
        return performance


    def get_marketers_performance(self, marketer_ids, start_day, end_day):
        """
        :returns: performance_data
        """
        performance = dict()
        for m in marketer_ids:
            path = 'marketers/{0}/performanceByDay'.format(m)
            performance[m] = dict()
            result = self._page_performance_data(path, start_day, end_day)
            print "starting data adding loop"
            for data in result:
                print "adding data to result"
                performance[m] = data
        return performance

    def get_publisher_performace_per_marketer(self, marketer_ids, start_day, end_day):
        """
        :returns: dict[marketer_id][publisher_id] = performance_data
        """
        performance = dict()
        for m in marketer_ids:
            path = 'marketers/{0}/performanceByPublisher'.format(m)
            performance[m] = dict()
            result = self._page_performance_data(path, start_day, end_day)
            for data in result:
                performance[m][data['id']] = data
        return performance

    def get_campaign_performace_per_section(self, campaign_ids, start_day, end_day):
        """
        :returns: dict[campaign_id][section] = performance_data
        """
        performance = dict()
        for c in campaign_ids:
            path = 'campaigns/{0}/performanceBySection'.format(c)
            performance[c] = dict()
            result = self._page_performance_data(path, start_day, end_day)
            for data in result:
                performance[c][data['id']] = data
        return performance

    def get_campaign_performace_per_day(self, campaign_ids, start_day, end_day):
        """
        :returns: dict[campaign_id][section] = performance_data
        """
        performance = dict()
        for c in campaign_ids:
            path = 'campaigns/{0}/performanceByDay'.format(c)
            performance[c] = dict()
            result = self._page_performance_data(path, start_day, end_day)
            print "start data adding loop"
            for data in result:
                print "adding data to result"
                performance[c] = data
        return performance

    # ----------------------------------------------------------------------------------------------
    # "Private" helper methods for acquiring/paging performance information
    # ----------------------------------------------------------------------------------------------
    def _page_performance_data(self, path, start, end):
        result = []
        offset = 0

        performance = self._get_performance_data(path, start, end, 50, offset)
        while performance:
            result.extend(performance)

            offset += len(performance)
            performance = self._get_performance_data(path, start, end, 50, offset)
        return result

    def _get_performance_data(self, path, start, end, limit, offset):
        if not start.tzinfo:
            start = start.replace(tzinfo=pytz.UTC)
        if not end.tzinfo:
            end = end.replace(tzinfo=pytz.UTC)
        start = start.astimezone(self.locale)
        end = end.astimezone(self.locale)

        params = {'limit': limit,
                  'offset': offset,
                  'from': start.strftime('%Y-%m-%d'),
                  'to': end.strftime('%Y-%m-%d')}
        result = self._request(path +"/", params)
        return result.get('details', [])

    # ----------------------------------------------------------------------------------------------
    # Methods to acquire promoted link information
    # ----------------------------------------------------------------------------------------------
    def get_promoted_link(self, promoted_link_id):
        path = 'promotedLinks/{id}'.format(id=promoted_link_id)
        result = self._request(path)
        return result

    def get_promoted_links_per_campaign(self, campaign_ids=[], enabled=None, statuses=[]):
        campaign_ids = campaign_ids or self.get_campaign_ids()
        promoted_links = dict()
        for c in campaign_ids:
            promoted_links[c] = self.get_promoted_links_for_campaign(c, enabled, statuses)
        return promoted_links

    def get_promoted_links_for_campaign(self, campaign_id, enabled=None, statuses=[]):
        return [link for link in self._yield_promoted_links_for_campaign(campaign_id, enabled, statuses)]

    def _yield_promoted_links_for_campaign(self, campaign_id, enabled=None, statuses=[]):
        offset = 0
        path = 'campaigns/{0}/promotedLinks'.format(campaign_id)
        promoted_links = self._page_promoted_links_for_campaign(path, enabled, statuses, 50, offset)
        while promoted_links:
            for pl in promoted_links:
                yield pl

            offset += len(promoted_links)
            promoted_links = self._page_promoted_links_for_campaign(path, enabled, statuses, 50, offset)

    def _page_promoted_links_for_campaign(self, path, enabled, statuses, limit, offset):
        params = {'limit': limit,
                  'offset': offset}

        if enabled is not None:
            params['enabled'] = 'true' if enabled else 'false'
        if statuses:
            params['statuses'] = ','.join(statuses)

        return self._request(path, params).get('promotedLinks', [])

    # ----------------------------------------------------------------------------------------------
    # Other methods
    # ----------------------------------------------------------------------------------------------
    def get_currencies(self):
        results = self._request('currencies')
        return results.get('currencies', [])
