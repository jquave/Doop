#!/bin/bash

hadoop com.sun.tools.javac.Main Wikilytics.java
jar cf wc.jar Wikilytics*.class
