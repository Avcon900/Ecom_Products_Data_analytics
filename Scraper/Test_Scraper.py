#Building a Selenium based webscraper for Ecommerce websites

from selenium import webdriver
from selenium.webdriver.common.by import By #helps find elements using By
from selenium.webdriver.common.keys import Keys #controls keyboard actions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains

import pandas as pd
import time

# defining categories
categories = ['controllers']

#elements to extract
product_title = []
Number_of_Reviews = []
Rating = []
Bought_last_month = []
Price_after_Discount = []
MRP = []
Product_Image_urls = []

#webdriver setup
driver = webdriver.Chrome()

#loop for each page
for category in range(len(categories)):
    for page in range(1,20):

        driver.get(f"https://www.amazon.in/s?k={categories[category]}&page={page}")
        time.sleep(7)
        wait = WebDriverWait(driver,10)


        #Extracting product titles
        product_titles = wait.until(
            EC.presence_of_all_elements_located
            ((By.CSS_SELECTOR, '[data-cy="title-recipe"] h2 span'))
        )

        product_title.extend([title.text for title in product_titles])

        #print(product_title)

        #Extracting Number of Reviews
        Review_num = wait.until(
            EC.presence_of_all_elements_located
            ((By.CSS_SELECTOR, '[data-cy="reviews-block"] a > span.a-size-base.s-underline-text'))
        )

        Number_of_Reviews.extend([num.text for num in Review_num])

        #print(Number_of_Reviews)

        #Extracting Ratings

        anchors = wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'a.a-popover-trigger.a-declarative'))
        )

        for j, anchor in enumerate(anchors):
            try:
                aria_label = anchor.get_attribute("aria-label")
                if aria_label:
                    Rating.append(aria_label)
                else:
                    Rating.append("Not found")
            except Exception as e:
                Rating.append("Error")

        #print(Rating)

        # Extracting sales in last month
        Last_month_sales = wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, '[data-cy="reviews-block"] span.a-size-base.a-color-secondary'))
        )

        Bought_last_month.extend([sale.text for sale in Last_month_sales])

        #print(Bought_last_month)

        #Extracting Price after Discount
        Post_Discount_Prices = wait.until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, 'a-price-whole'))
        )

        Price_after_Discount.extend([p.text for p in Post_Discount_Prices])

        #print(Price_after_Discount)

        #Extracting MRP
        Actual_Price = wait.until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, 'a-text-price'))
        )

        MRP.extend([m.text for m in Actual_Price])

        #print(MRP)

        #Extracting Product Images
        images = wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div.a-section.aok-relative.s-image-fixed-height img'))
        )


        for i, img in enumerate(images):
            img_url = img.get_attribute('src')
            if img_url:
                Product_Image_urls.append(img_url)

        # print(Product_Image_urls)
        
        time.sleep(7)

driver.quit()

df = pd.DataFrame({
    "Title": product_title,
    "Reviews": Number_of_Reviews,
    "Rating": Rating,
    "Bought Last Month": Bought_last_month,
    "Price After Discount": Price_after_Discount,
    "MRP": MRP,
    "Image URL": Product_Image_urls
})
df['Product ID'] = range(1, len(df) + 1)

df.to_csv("ecommerce_scraped_data.csv", index=False)