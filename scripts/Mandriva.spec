%define git_repo psycopg2
%define git_head HEAD
%define module psycopg2

Summary:        PostgreSQL database adapter for Python
Name:           python-%module
Version:        %git_get_ver
Release:        %mkrel %git_get_rel2
Group:          Development/Python
License:        GPLv2 and ZPLv2.1 and BSD
URL:            http://www.initd.org/software/initd/psycopg
Source0:        %git_bs_source %{name}-%{version}.tar.gz
# for DateTime
Requires:       python-egenix-mx-base
BuildRequires:  python-devel
BuildRequires:  postgresql-devel
BuildRequires:  python-egenix-mx-base

%description
psycopg is a PostgreSQL database adapter for the Python programming
language (just like pygresql and popy.) It was written from scratch with
the aim of being very small and fast, and stable as a rock. The main
advantages of psycopg are that it supports the full Python DBAPI-2.0 and
being thread safe at level 2.

psycopg2 is an almost complete rewrite of the psycopg 1.1.x branch.

%prep
%git_get_source
%setup -q

%build
export CFLAGS="%{optflags}"
python setup.py build

%install
python setup.py install --root=$RPM_BUILD_ROOT

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%doc AUTHORS examples/ ChangeLog LICENSE README
%py_platsitedir/psycopg2*

%changelog -f %{_sourcedir}/%{name}-changelog.gitrpm.txt