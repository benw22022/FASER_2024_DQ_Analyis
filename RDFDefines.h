#ifndef RDFDefines
#define RDFDefines

#include "ROOT/RDataFrame.hxx"
#include "ROOT/RVec.hxx"

using namespace ROOT::VecOps;

template<typename T>
RVec<T> DeltaTheta(const RVec<T>& theta1, const RVec<T>& theta2) {
  return DeltaPhi(theta1, theta2);
}

double InvariantMass(double pt1, double eta1, double phi1, double mass1,
		     double pt2, double eta2, double phi2, double mass2) {


  ROOT::Math::PtEtaPhiMVector p1 (pt1, eta1, phi1, mass1);
  ROOT::Math::PtEtaPhiMVector p2 (pt2, eta2, phi2, mass2);

  return (p1 + p2).M();
}

double InvariantMassAlt(double px1, double py1, double pz1, double e1,
			double px2, double py2, double pz2, double e2) {


  ROOT::Math::PxPyPzEVector p1 (px1, py1, pz1, e1);
  ROOT::Math::PxPyPzEVector p2 (px2, py2, pz2, e2);

  return (p1 + p2).M();
}

double Max4(double v1 = 0, double v2 = 0, double v3 = 0, double v4 = 0) {
  RVec<double> vec = {v1, v2, v3, v4};
  return Max(vec);
}

template<typename T>
RVec<T> MaxFrom(RVec<T> v1, RVec<T> v2) {
  auto v = RVec<T>(v1.size());

  for (int i(0); i < v1.size(); i++) {
    v[i] = (v1[i] < v2[i]) ? v2[i] : v1[i]; 
  }

  return v;
}

double Radius(double x, double y) {
  return sqrt(x * x + y * y);
}

template<typename T>
RVec<T> Radius(RVec<T> x, RVec<T> y) {
  return sqrt(x * x + y * y);
}


// need to take into account sign of px and py when calculating phi. There is likely a less confusing way of doing this, but I (deion) tried several and couldn't get them to work 
double Phi(double px, double py) {
  return (-(3.1415/2.0)*(py/abs(py))+atan(-px/py)+3.1415);
}

template<typename T>
RVec<T> Phi(RVec<T> px, RVec<T> py) {
  return (-(3.1415/2.0)*(py/abs(py))+atan(-px/py)+3.1415);
}


template<typename T>
RVec<bool> RemoveDuplicates(RVec<T> vec, float eps = 1e-15) {
  
  
  RVec<bool> res(vec.size(), true);

  for (int i(0); i < vec.size(); i++) {
    for (int j(0); j < i; j++) { 
      if (abs(vec[i] - vec[j]) < eps) 
	res[i] = false;
    }
  }

  return res;
}

#endif
