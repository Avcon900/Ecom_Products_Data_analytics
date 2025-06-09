#Building a Selenium based webscraper for Ecommerce websites

from selenium import webdriver
from selenium.webdriver.common.by import By #helps find elements using By
from selenium.webdriver.common.keys import Keys #controls keyboard actions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains

import pandas
import time

#elements to extract
product_titles = []
Number_of_Reviews = []
Rating = []


#webdriver setup
driver = webdriver.Chrome()

driver.get("https://www.amazon.in/s?k=controllers&page=1")

wait = WebDriverWait(driver,10)


#Extracting product titles
product_titles = wait.until(
    EC.presence_of_all_elements_located
    ((By.CSS_SELECTOR, '[data-cy="title-recipe"] h2 span'))
)

product_title = [title.text for title in product_titles]

#print(product_title)

#Extracting Number of Reviews
Review_num = wait.until(
    EC.presence_of_all_elements_located
    ((By.CSS_SELECTOR, '[data-cy="reviews-block"] a > span.a-size-base.s-underline-text'))
)

Number_of_Reviews = [num.text for num in Review_num]

#print(Number_of_Reviews)

#Extracting Ratings

anchors = wait.until(
    EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'a.a-popover-trigger.a-declarative'))
)

for i, anchor in enumerate(anchors):
    try:
        aria_label = anchor.get_attribute("aria-label")
        if aria_label:
            print(f"Rating {i+1}: {aria_label}")
            Rating.append(aria_label)
        else:
            print(f"Rating {i+1}: Not found")
            Rating.append("Not found")
    except Exception as e:
        Rating.append("Error")

#print(Rating)

driver.quit()