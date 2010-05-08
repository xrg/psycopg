%define module psycopg2

Summary:        PostgreSQL database adapter for Python
Name:           python-%module
Version:        2.0.13
Release:        %mkrel 1
Group:          Development/Python
License:        GPLv2 and ZPLv2.1 and BSD
URL:            http://www.initd.org/software/initd/psycopg
Source0:        http://initd.org/pub/software/psycopg/%{module}-%{version}.tar.gz
Source1:        http://initd.org/pub/software/psycopg/%{module}-%{version}.tar.gz.asc
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root
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
%setup -q -n %{module}-%{version}

%build
export CFLAGS="$RPM_OPT_FLAGS"
python setup.py build

%install
python setup.py install --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES

%clean
rm -rf $RPM_BUILD_ROOT

%files -f INSTALLED_FILES
%defattr(-,root,root,-)
%doc AUTHORS examples/ ChangeLog  LICENSE  README
