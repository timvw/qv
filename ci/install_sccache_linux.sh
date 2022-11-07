LINK=${LINK:-'https://github.com/mozilla/sccache/releases/download'}
SCCACHE_VERSION=${SCCACHE_VERSION:-'v0.3.0'}

SCCACHE_FILE=sccache-$SCCACHE_VERSION-x86_64-unknown-linux-musl
mkdir -p $HOME/.local/bin
curl -L "$LINK/$SCCACHE_VERSION/$SCCACHE_FILE.tar.gz" | tar xz
mv -f $SCCACHE_FILE/sccache $HOME/.local/bin/sccache
chmod a+x $HOME/.local/bin/sccache
echo "$HOME/.local/bin" >> $GITHUB_PATH