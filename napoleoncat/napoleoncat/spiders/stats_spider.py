import scrapy
import pandas as pd


class StatSpider(scrapy.Spider):

    name = 'stats'

    df = pd.read_excel(r'countries.xlsx')
    start_urls = list(df.url)

    df_errors = pd.DataFrame(columns=["error", "url"])

    def parse(self, response):
        print("Url:", response.url)
        try:
            # get dates
            dates = response.xpath('//ul[@class="archive"]/li/a/@href').getall()
            for date in dates:
                yield scrapy.Request(date, self.parse_date)
        except:
            print("Couldn't get url.", response.url)
            df_errors = df_errors.append({'error': "Couldn't get link", 'url': response.url}, ignore_index=True)

    def parse_date(self, response):
        try:
            # get social media platforms
            sm_platforms = response.xpath('//ul[@class="country-channels"]/li/a/@href').getall()
            for smp in sm_platforms:
                if smp != '#':
                    yield scrapy.Request(smp, self.parse_smp)
        except:
            print("Couldn't parse date.", response.url)
            df_errors = df_errors.append({'error': "Couldn't parse date", 'url': response.url}, ignore_index=True)

    def parse_smp(self, response):
        try:
            # find out which platform
            title = response.xpath('//div[@class="visualize-header--title"]/strong/text()').get()
            country = title.split(' in ')[1].replace(",", "-")
            date = response.xpath('//div[@class="visualize-header--title"]/span/text()').get()
            platform = title.split(' ')[0]

            bars = response.css('div.visualize-chart--gender.visualize-chart--gender-hint::attr(title)').getall()
            if platform == "Linkedin":
                for bar in bars:
                    gender = 'Not Available'
                    age = bar.split(':')[0]
                    followers = bar.split(':')[1].split('(')[0].replace(' ', '')
                    yield {
                        'country': country,
                        'date': date,
                        'platform': platform,
                        'gender': gender,
                   *     'age': age,
                        'followers': followers
                    }
            else:
                for bar in bars:
                    gender = bar.split(' ')[0]
                    age = bar.split(' ')[1].replace(':', '')
                    followers = bar.split(': ')[1].split(' (')[0].replace(' ', '')
                    yield {
                        'country': country,
                        'date': date,
                        'platform': platform,
                        'gender': gender,
                        'age': age,
                        'followers': followers
                    }
        except:
            print("Couldn't parse SMP.", response.url)
            df_errors = df_errors.append({'error': "Couldn't parse SMP", 'url': response.url}, ignore_index=True)

    def close(self, reason):
        self.df_errors.to_excel(r'errors.xlsx', index=False)
        print("Finished.")
        super().close(reason)