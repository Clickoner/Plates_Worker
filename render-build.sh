#!/usr/bin/env bash
set -e

echo "Installing Ghostscript..."
apt-get update
apt-get install -y ghostscript
