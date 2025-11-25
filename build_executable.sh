#!/bin/bash
# Copyright 2025 deep-bi
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

echo "Building starrocks-br executable..."

if ! command -v pyinstaller &> /dev/null; then
    echo "Installing PyInstaller..."
    pip install pyinstaller
fi

echo "Creating executable..."
pyinstaller --onefile --name starrocks-br --paths src entry_point.py

echo ""
echo "✓ Build complete!"
echo "✓ Executable location: $(pwd)/dist/starrocks-br"
echo ""
echo "You can now distribute this executable to your client."
echo "They can run it directly without Python or any dependencies:"
echo "  ./dist/starrocks-br --help"
