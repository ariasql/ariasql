# Bundles asql binaries
# Usage: ./bundler.sh

VERSION=ALPHA

echo "Bundling asql binaries into bin "

(cd src && GOOS=darwin GOARCH=amd64 go build -o ../bin/macos-darwin/amd64/asql && tar -czf ../bin/macos-darwin/amd64/asql-$VERSION-amd64.tar.gz -C ../bin/macos-darwin/amd64/ $(ls  ../bin/macos-darwin/amd64/))
(cd src && GOOS=darwin GOARCH=arm64 go build -o ../bin/macos-darwin/arm64/asql && tar -czf ../bin/macos-darwin/arm64/asql-$VERSION-arm64.tar.gz -C ../bin/macos-darwin/arm64/ $(ls  ../bin/macos-darwin/arm64/))
(cd src && GOOS=linux GOARCH=386 go build -o ../bin/linux/386/asql && tar -czf ../bin/linux/386/asql-$VERSION-386.tar.gz -C ../bin/linux/386/ $(ls  ../bin/linux/386/))
(cd src && GOOS=linux GOARCH=amd64 go build -o ../bin/linux/amd64/asql && tar -czf ../bin/linux/amd64/asql-$VERSION-amd64.tar.gz -C ../bin/linux/amd64/ $(ls  ../bin/linux/amd64/))
(cd src && GOOS=linux GOARCH=arm go build -o ../bin/linux/arm/asql && tar -czf ../bin/linux/arm/asql-$VERSION-arm.tar.gz -C ../bin/linux/arm/ $(ls  ../bin/linux/arm/))
(cd src && GOOS=linux GOARCH=arm64 go build -o ../bin/linux/arm64/asql && tar -czf ../bin/linux/arm64/asql-$VERSION-arm64.tar.gz -C ../bin/linux/arm64/ $(ls  ../bin/linux/arm64/))
(cd src && GOOS=freebsd GOARCH=arm go build -o ../bin/freebsd/arm/asql && tar -czf ../bin/freebsd/arm/asql-$VERSION-arm.tar.gz -C ../bin/freebsd/arm/ $(ls  ../bin/freebsd/arm/))
(cd src && GOOS=freebsd GOARCH=amd64 go build -o ../bin/freebsd/amd64/asql && tar -czf ../bin/freebsd/amd64/asql-$VERSION-amd64.tar.gz -C ../bin/freebsd/amd64/ $(ls  ../bin/freebsd/amd64/))
(cd src && GOOS=freebsd GOARCH=386 go build -o ../bin/freebsd/386/asql && tar -czf ../bin/freebsd/386/asql-$VERSION-386.tar.gz -C ../bin/freebsd/386/ $(ls  ../bin/freebsd/386/))
(cd src && GOOS=windows GOARCH=amd64 go build -o ../bin/windows/amd64/asql.exe && zip -r -j ../bin/windows/amd64/asql-$VERSION-x64.zip ../bin/windows/amd64/asql.exe)
(cd src && GOOS=windows GOARCH=arm64 go build -o ../bin/windows/arm64/asql.exe && zip -r -j ../bin/windows/arm64/asql-$VERSION-x64.zip ../bin/windows/arm64/asql.exe)
(cd src && GOOS=windows GOARCH=386 go build -o ../bin/windows/386/asql.exe && zip -r -j ../bin/windows/386/asql-$VERSION-x86.zip ../bin/windows/386/asql.exe)

echo "Done"