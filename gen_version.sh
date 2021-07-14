#!/bin/bash

version=`git describe`

cat << EOF
package main

const version = "$version"
EOF
