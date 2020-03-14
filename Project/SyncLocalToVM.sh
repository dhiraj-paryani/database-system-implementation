echo "$#"

PARAM="-anv"

if [ "$#" -eq 1 ]; then
  PARAM="-av"
fi

rsync ${PARAM} \
  --exclude='.git' \
  --exclude='cmake-build-debug' \
  --exclude='.idea' \
  --exclude='*.bin' \
  --exclude='*.o' \
  --exclude='*.bin.bigq' \
  --exclude='*.out' \
  --exclude='lex.yy.c' \
  --exclude='y.tab.*' \
  --exclude='.DS_Store' \
  --exclude='main' \
  ./ msodhani@storm.cise.ufl.edu:~/dhiraj/Project/
#make main
#make a1test.out
#make test.out

echo ""
echo ""
echo ""
if [ "$#" -eq 1 ]; then
  echo "############################################################"
  echo "Sync completed"
  echo "############################################################"
else
  echo "############################################################"
  echo "This is only diff, pass 1 argument to complete the sync"
  echo "############################################################"
fi
