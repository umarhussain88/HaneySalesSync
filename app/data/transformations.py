# 	Order and remove columns.
# 	Paste into google sheet.
# 	Filter out records without email addresses.
# 	Match with Shopify to ensure they aren't pre-existing customers.
# These are filtered out.
# 	Brett tells Hannah what the ZI search is.
# @brett
#  is this to be a human fillable field? there's no way to automate this I assume.
# 	Group contacts by company.
# This is to find the decision maker amongst a group of contacts.
# 	Check that the phone number is within the US.
# If any phone is not united states then exclude.
# This is to stop any sample orders going out to foreign countries.


target_schema = {
    "first_name": "first_name",
    "last_name": "last_name",
    "title": "job_title",
    "role": "job_function",
    "email": "email_address",
    "linkedin": "linkedin_contact_profile_url",
    "company": "company_name",
    "phone": "direct_phone_number",
    "phone_1": "direct_phone_number",
}
