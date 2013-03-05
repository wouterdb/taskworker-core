<?xml version="1.0" encoding="UTF-8"?>
<!--


        Copyright 2013 KU Leuven Research and Development - iMinds - Distrinet

        Licensed under the Apache License, Version 2.0 (the "License");
        you may not use this file except in compliance with the License.
        You may obtain a copy of the License at

            http://www.apache.org/licenses/LICENSE-2.0

        Unless required by applicable law or agreed to in writing, software
        distributed under the License is distributed on an "AS IS" BASIS,
        WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
        See the License for the specific language governing permissions and
        limitations under the License.

        Administrative Contact: dnet-project-office@cs.kuleuven.be
        Technical Contact: bart.vanbrabant@cs.kuleuven.be

-->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
	xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:fo="http://www.w3.org/1999/XSL/Format"
	xmlns:inv="http://www.quattroclix.com/invoice" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xmlns:date="http://exslt.org/dates-and-times"
	xsi:schemaLocation="http://www.quattroclix.com/invoice http://www.w3.org/1999/ XSL/Format http://www.xmlblueprint.com/documents/fop.xsd"
	exclude-result-prefixes="xs inv" extension-element-prefixes="date"
	version="1.0">

	<xsl:output indent="yes" />

	<xsl:template match="/invoice">

		<fo:root xmlns:fo="http://www.w3.org/1999/XSL/Format">
			<!-- A4: 217mm x 297mm body: 180mm x 280mm -->
			<fo:layout-master-set>
				<fo:simple-page-master margin-bottom="7mm"
					margin-left="19mm" margin-right="18mm" margin-top="1cm"
					master-name="invoice-page-master">
					<fo:region-body margin-bottom="20mm" margin-top="60mm" />
					<fo:region-before extent="30mm" />
					<fo:region-after extent="10mm" />
				</fo:simple-page-master>
			</fo:layout-master-set>
			<fo:page-sequence master-reference="invoice-page-master">
				<fo:static-content flow-name="xsl-region-before">
					<fo:block-container absolute-position="absolute"
						left="120mm" top="Omm">
						<fo:block>
							<fo:external-graphic src="url('logo2.png')" />
						</fo:block>
					</fo:block-container>
				</fo:static-content>

				<fo:static-content flow-name="xsl-region-after">
					<fo:block font-size="10pt" border-top="solid"
						border-top-color="black" padding-top="2mm">
						<fo:inline>IMP, Celestijnenlaan 200A, 3001 Leuven BTW: BE 0853.134.654</fo:inline>
					</fo:block>
					<fo:block font-size="10pt">
						<fo:inline>IBAN: 1234123412341234 BIC: BEBBAZER</fo:inline>
					</fo:block>
				</fo:static-content>

				<fo:flow flow-name="xsl-region-body">
					<fo:table width="100%">
						<fo:table-column column-width="50%" />
						<fo:table-column column-width="50%" />
						<fo:table-body>
							<fo:table-row>
								<fo:table-cell>
									<fo:table width="75%" font-size="16pt">
										<fo:table-column column-width="40%" />
										<fo:table-column column-width="60%" />
										<fo:table-body>
											<fo:table-row>
												<fo:table-cell>
													<fo:block font-size="10pt">Invoice</fo:block>
												</fo:table-cell>
												<fo:table-cell>
													<fo:block font-size="10pt">${invoice}</fo:block>
												</fo:table-cell>
											</fo:table-row>
											<fo:table-row>
												<fo:table-cell>
													<fo:block font-size="10pt">Date</fo:block>
												</fo:table-cell>
												<fo:table-cell>
													<fo:block font-size="10pt">${date}</fo:block>
												</fo:table-cell>
											</fo:table-row>

										</fo:table-body>
									</fo:table>

								</fo:table-cell>
								<fo:table-cell>
									<fo:block font-size="14pt">
										<fo:block>${name}</fo:block>
										<fo:block>${street}</fo:block>
										<fo:block>${zipcode} ${city}</fo:block>
										<fo:block>${email}</fo:block>
									</fo:block>
								</fo:table-cell>
							</fo:table-row>
						</fo:table-body>
					</fo:table>

					<fo:block padding-top="20mm">
						<fo:block font-family="sans-serif" font-size="10pt"
							margin-top="10mm">
							<fo:table border-style="solid" table-layout="fixed"
								width="100%">
								<fo:table-column column-width="150mm" />
								<fo:table-column column-width="23mm" />
								<fo:table-header background-color="silver"
									text-align="center">
									<fo:table-row>
										<fo:table-cell border-style="solid" padding="1mm">
											<fo:block font-weight="bold">Product Description</fo:block>
										</fo:table-cell>
										<fo:table-cell border-style="solid" padding="1mm">
											<fo:block font-weight="bold">Price</fo:block>
										</fo:table-cell>
									</fo:table-row>
								</fo:table-header>
								<fo:table-body>
									<fo:table-row>
										<fo:table-cell padding="1mm">
											<fo:block>${product}</fo:block>
										</fo:table-cell>
										<fo:table-cell padding="1mm">
											<fo:block text-align="right">${total} eur</fo:block>
										</fo:table-cell>
									</fo:table-row>
									<fo:table-row>
										<fo:table-cell number-columns-spanned="2"
											padding="1mm" padding-top="2mm">
											<fo:block font-style="backslant" text-align="right"
												font-weight="bold">
												<fo:inline padding-right="7mm">Total</fo:inline>
												<fo:inline>${total} eur</fo:inline>
											</fo:block>
										</fo:table-cell>
									</fo:table-row>
								</fo:table-body>
							</fo:table>
						</fo:block>
					</fo:block>
				</fo:flow>

			</fo:page-sequence>
		</fo:root>
	</xsl:template>
</xsl:stylesheet>