// Copyright CERN and copyright holders of ALICE O2. This software is
// distributed under the terms of the GNU General Public License v3 (GPL
// Version 3), copied verbatim in the file "COPYING".
//
// See http://alice-o2.web.cern.ch/license for full licensing information.
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

/// @file   AliAlgConstraint.h
/// @author ruben.shahoyan@cern.ch, michael.lettrich@cern.ch
/// @since  2021-02-01
/// @brief  Descriptor of geometrical constraint

/**
 *  Descriptor of geometrical constraint: the cumulative
 *  corrections of children for requested DOFs in the frame of
 *  parent (of LAB if parent is not defined) forced to be 0.
 *  The parent - child relationship need not to be real
 *
 *  Constraint wil be quazi-exact (Lagrange multiplier) if
 *  corresponding sigma = 0, or of gaussian type is sigma>0
 */

#ifndef ALIALGCONSTRAINT_H
#define ALIALGCONSTRAINT_H

#include <stdio.h>
#include <TNamed.h>
#include <TObjArray.h>
#include "Align/AliAlgVol.h"

namespace o2
{
namespace align
{

class AliAlgConstraint : public TNamed
{
 public:
  enum { kNDOFGeom = AliAlgVol::kNDOFGeom };
  enum { kNoJacobianBit = BIT(14) };
  //
  AliAlgConstraint(const char* name = 0, const char* title = 0);
  virtual ~AliAlgConstraint();
  //
  void SetParent(const AliAlgVol* par);
  const AliAlgVol* GetParent() const { return fParent; }
  //
  int GetNChildren() const { return fChildren.GetEntriesFast(); }
  AliAlgVol* GetChild(int i) const { return (AliAlgVol*)fChildren[i]; }
  void AddChild(const AliAlgVol* v)
  {
    if (v)
      fChildren.AddLast((AliAlgVol*)v);
  }
  //
  bool IsDOFConstrained(int dof) const { return fConstraint & 0x1 << dof; }
  uint8_t GetConstraintPattern() const { return fConstraint; }
  void ConstrainDOF(int dof) { fConstraint |= 0x1 << dof; }
  void UnConstrainDOF(int dof) { fConstraint &= ~(0x1 << dof); }
  void SetConstrainPattern(uint32_t pat) { fConstraint = pat; }
  bool HasConstraint() const { return fConstraint; }
  double GetSigma(int i) const { return fSigma[i]; }
  void SetSigma(int i, double s = 0) { fSigma[i] = s; }
  //
  void SetNoJacobian(bool v = true) { SetBit(kNoJacobianBit, v); }
  bool GetNoJacobian() const { return TestBit(kNoJacobianBit); }
  //
  void ConstrCoefGeom(const TGeoHMatrix& matRD, float* jac /*[kNDOFGeom][kNDOFGeom]*/) const;
  //
  virtual void Print(const Option_t* opt = "") const;
  virtual void WriteChildrenConstraints(FILE* conOut) const;
  virtual void CheckConstraint() const;
  virtual const char* GetDOFName(int i) const { return AliAlgVol::GetGeomDOFName(i); }
  //
 protected:
  // ------- dummies -------
  AliAlgConstraint(const AliAlgConstraint&);
  AliAlgConstraint& operator=(const AliAlgConstraint&);
  //
 protected:
  uint32_t fConstraint;     // bit pattern of constraint
  double fSigma[kNDOFGeom]; // optional sigma if constraint is gaussian
  const AliAlgVol* fParent; // parent volume for contraint, lab if 0
  TObjArray fChildren;      // volumes subjected to constraints
  //
  ClassDef(AliAlgConstraint, 2);
};

} // namespace align
} // namespace o2
#endif
