#!/bin/sh

cat <<EOF > .launcher.tmp
#!/bin/sh
MYSELF=\$(which "\$0" 2>/dev/null)
[ \$? -gt 0 -a -f "\$0" ] && MYSELF="./\$0"
java=java
if test -n "\$JAVA_HOME"; then
    java="\$JAVA_HOME/bin/java"
fi
exec "\$java" \$java_args -Xmx2g -jar \$MYSELF "\$@"
exit 1
EOF

cat .launcher.tmp $1 > $2

chmod +x $2
