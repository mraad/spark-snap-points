BEGIN{
  OFS=","
  XMIN=-106.644292
  YMIN=25.84096
  XMAX=-93.518721
  YMAX=36.500662
  DX=XMAX-XMIN
  DY=YMAX-YMIN
  srand()
  for(I=0;I<2000000;I++){
    X=XMIN+rand()*DX
    Y=YMIN+rand()*DY
    printf "%d,%.6f,%.6f\n",I,X,Y
  }
}
