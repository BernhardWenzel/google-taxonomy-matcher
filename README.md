google-taxonomy-matcher
=======================

Matches a category of Google's Taxonomy to product that is described in any kind of text data

It parses an input csv file (`product.csv`) and assigns one of the categories that are found in Google's taxonomy and writes the result back to another csv file (`product.matched.csv`).

## Usage

	> python matcher.py  -h
	usage: matcher.py [-h] [-o [OVERWRITE]] [bc [bc ...]]

	Finds category based on Google's taxonomy in a product description

	positional arguments:
	  bc                    The base categories of the product. Can speed up execution a lot. Example: matcher "Furniture" "Home & Garden"

	optional arguments:
	  -h, --help            show this help message and exit
	  -o [OVERWRITE], --overwrite [OVERWRITE]
	                        If set category column in product file will be
	                        overwritten

## Configuration

This script can be tweaked further. Copy `settings.sample.yaml` to `settings.yaml` and adjust accordingly.

## Read more

More info about how it works can be found at [http://www.bernhardwenzel.com/blog/2013/08/26/google-shopping-taxonomy-algorithm/](http://www.bernhardwenzel.com/blog/2013/08/26/google-shopping-taxonomy-algorithm/)
