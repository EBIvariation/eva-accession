from lxml import etree as et

#TODO: Move method to pyutils repository

def get_properties(profile, xml_path):
    tree = et.parse(xml_path)
    root = tree.getroot()

    properties = {}
    for child in root.xpath('//settings/profiles/profile/id[text()="' + profile + '"]/../properties/*'):
        properties[child.tag] = child.text
    return properties